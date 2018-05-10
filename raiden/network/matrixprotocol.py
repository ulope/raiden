import binascii
import json
import logging
from collections import namedtuple
from enum import Enum
from operator import itemgetter
from random import Random
from typing import Dict, Set
from urllib.parse import urlparse

import gevent
from ethereum import slogging
from gevent.event import AsyncResult
from matrix_client.errors import MatrixRequestError
from matrix_client.user import User

import raiden
from raiden.constants import ID_TO_NETWORKNAME
from raiden.encoding import signing
from raiden.exceptions import (
    InvalidAddress,
    UnknownAddress,
    UnknownTokenAddress,
)
from raiden.messages import (
    Ping,
    Processed,
    SignedMessage,
    decode as message_from_bytes,
    from_dict as message_from_dict
)
from raiden.network.utils import get_http_rtt
from raiden.transfer.state import NODE_NETWORK_REACHABLE, NODE_NETWORK_UNREACHABLE
from raiden.transfer.state_change import ActionChangeNodeNetworkState
from raiden.udp_message_handler import on_udp_message
from raiden.utils import (
    address_decoder,
    address_encoder,
    data_decoder,
    data_encoder,
    eth_sign_sha3,
    isaddress,
    pex,
    sha3,
    typing
)
from raiden_libs.network.matrix import GMatrixClient, Room


log = slogging.get_logger(__name__)

SentMessageState = namedtuple('SentMessageState', (
    'async_result',
    'receiver_address',
))


class UserPresence(Enum):
    ONLINE = 'online'
    UNAVAILABLE = 'unavailable'
    OFFLINE = 'offline'


class RaidenMatrixProtocol:
    _room_prefix = 'raiden'
    _room_sep = '_'

    def __init__(self, raiden_service: 'raiden.raiden_service.RaidenService'):
        self.raiden_service: 'raiden.raiden_service.RaidenService' = raiden_service
        self._server_name: str = self._select_server()
        self.client: GMatrixClient = GMatrixClient(self._server_name)

        self.discovery_room: Room = None

        self._senthashes_to_states = dict()
        self._receivedhashes_to_processedmessages = dict()
        self._userid_to_presence: Dict[str, UserPresence] = dict()
        self._userids_to_address: Dict[str, str] = dict()
        self._address_to_roomid_cache: Dict[typing.Address, str] = dict()

    @property
    def network_name(self) -> str:
        return ID_TO_NETWORKNAME.get(
            self.raiden_service.network_id,
            str(self.raiden_service.network_id)
        )

    def start(self):
        self._login_or_register()

        discovery_room_config = self.raiden_service.config['matrix']['discovery_room']
        discovery_room_alias = self._make_room_alias(discovery_room_config['alias_fragment'])
        discovery_room_alias_full = f'#{discovery_room_alias}:{discovery_room_config["server"]}'
        try:
            discovery_room = self.client.join_room(discovery_room_alias_full)
        except MatrixRequestError as ex:
            if ex.code != 404:
                raise
            # Room doesn't exist
            own_server_hostname = urlparse(self._server_name).hostname
            if discovery_room_config['server'] != own_server_hostname:
                raise RuntimeError(
                    f"Discovery room {discovery_room_alias_full} not found and can't be created on"
                    f" a federated homeserver."
                )
            discovery_room = self.client.create_room(discovery_room_alias, is_public=True)
        self.discovery_room = discovery_room
        # Populate initial members
        self.discovery_room.get_joined_members()

        # Iterate over a copy so we can modify the room list
        for room_id, room in list(self.client.get_rooms().items()):
            self._ensure_room_alias(room)
            if not room.canonical_alias:
                # Leave any rooms that don't have a canonical alias as they are not part of the
                # protocol
                room.leave()
                continue
            # Don't listen for messages in the discovery room
            if room.canonical_alias != discovery_room_alias_full:
                room.add_listener(self._handle_message)
            log.debug(
                'JOINED ROOM',
                room_id=room_id,
                aliases=room.aliases
            )

        self.client.add_invite_listener(self._handle_invite)
        self.client.add_presence_listener(self._handle_presence_change)
        self.discovery_room.add_member_state_listener(self._handle_discovery_membership_change)
        # TODO: Add (better) error handling strategy
        self.client.start_listener_thread(exception_handler=lambda e: None)
        # TODO: Add greenlet that regularly refreshes our presence state
        self.client.set_presence_state(UserPresence.ONLINE.value)

    def _login_or_register(self):
        # password is signed server address
        password = data_encoder(
            signing.sign(self._server_name.encode(), self.raiden_service.private_key))
        seed = int.from_bytes(signing.sign(b'seed', self.raiden_service.private_key)[-32:], 'big')
        rand = Random()  # deterministic, random secret for username suffixes
        rand.seed(seed)
        # try login and register on first 5 possible accounts
        for i in range(5):
            base_username = address_encoder(self.raiden_service.address)
            username = base_username
            if i:
                username = f'{username}.{rand.randint(0, 0xffffffff):08x}'

            try:
                token = self.client.login_with_password(username, password)
                log.info(
                    'LOGIN',
                    username=username,
                    password=password,
                    token=token,
                )
                break
            except MatrixRequestError as ex:
                if ex.code != 403:
                    raise
                log.debug(
                    'Could not login. Trying register',
                    username=username,
                    password=password,
                )
                try:
                    token = self.client.register_with_password(username, password)
                    log.info(
                        'REGISTER',
                        username=username,
                        password=password,
                        token=token,
                    )
                    break
                except MatrixRequestError as ex:
                    if ex.code != 400:
                        raise
                    log.debug('Username taken. Continuing')
                    continue
        else:
            raise ValueError('Could not register or login!')
        # TODO: persist access_token, to avoid generating a new login every time
        name = data_encoder(
            signing.sign(
                self.client.user_id.encode(),
                self.raiden_service.private_key,
                hasher=eth_sign_sha3
            )
        )
        self.client.get_user(self.client.user_id).set_display_name(name)

    def start_health_check(self, node_address):
        users = [
            user
            for user
            in self.client.search_user_directory(address_encoder(node_address))
            if _validate_userid_signature(user)
        ]
        existing = {presence['user_id'] for presence in self.client.get_presence_list()}
        user_ids = {u.user_id for u in users} - existing
        if user_ids:
            log.debug('Add to presence list', added_users=user_ids)
            self.client.modify_presence_list(add_user_ids=list(user_ids))

    def _handle_invite(self, room_id, state):
        """ Join all invited rooms """
        room = self.client.join_room(room_id)
        self._ensure_room_alias(room)
        room.add_listener(self._handle_message)
        log.debug(
            'Invited to room',
            room_id=room_id,
            aliases=room.aliases
        )

    def _handle_message(self, room, event):
        """ Handle text messages sent to listening rooms """
        if event['type'] != 'm.room.message' or event['content']['msgtype'] != 'm.text':
            # Ignore non-messages and non-text messages
            return

        sender_id = event['sender']

        if sender_id == self.client.user_id:
            # Ignore our own messages
            return

        user = self.client.get_user(sender_id)

        addr_display = self._userids_to_address.get(sender_id)
        if not addr_display:
            try:
                # recover displayname signature
                addr_display = signing.recover(
                    sender_id.encode(),
                    signature=data_decoder(user.get_display_name()),
                    hasher=eth_sign_sha3
                )
            except AssertionError:
                log.warning('INVALID MESSAGE', sender_id=sender_id)
                return
            if address_encoder(addr_display).lower() not in sender_id:
                log.warning(
                    'INVALID SIGNATURE',
                    peer_address=address_encoder(addr_display),
                    sender_id=sender_id
                )
                return
            self._userids_to_address[sender_id] = addr_display

        data = event['content']['body']
        if data.startswith('0x'):
            message = message_from_bytes(data_decoder(data))
        else:
            message = message_from_dict(json.loads(data))

        if isinstance(message, SignedMessage) and not message.sender:
            # FIXME: This can't be right
            message.sender = addr_display

        echohash = sha3(data.encode() + self.raiden.address)
        if echohash in self._receivedhashes_to_processedmessages:
            self.maybe_send_processed(*self._receivedhashes_to_processedmessages[echohash])
            return

        if isinstance(message, Processed):
            self._receive_processed(message)
        elif isinstance(message, Ping):
            log.warning(
                'Not required Ping received',
                message=data,
            )
        elif isinstance(message, SignedMessage):
            self._receive_message(message, echohash)
        elif log.isEnabledFor(logging.ERROR):
            log.error(
                'Invalid message',
                message=data,
            )

    def _receive_processed(self, processed):
        waitprocessed = self._senthashes_to_states.pop(processed.echo, None)

        if waitprocessed is None:
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    '`Processed` MESSAGE UNKNOWN ECHO',
                    node=pex(self.raiden_service.address),
                    echohash=pex(processed.echo),
                )

        else:
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    '`Processed` MESSAGE RECEIVED',
                    node=pex(self.raiden_service.address),
                    receiver=pex(waitprocessed.receiver_address),
                    echohash=pex(processed.echo),
                )

            waitprocessed.async_result.set(True)

    def _receive_message(self, message, echohash):
        is_debug_log_enabled = log.isEnabledFor(logging.DEBUG)

        if is_debug_log_enabled:
            log.info(
                'MESSAGE RECEIVED',
                node=pex(self.raiden_service.address),
                echohash=pex(echohash),
                message=message,
                message_sender=pex(message.sender)
            )

        try:
            on_udp_message(self.raiden_service, message)

            # only send the Processed message if the message was handled without exceptions
            processed_message = Processed(
                self.raiden_service.address,
                echohash,
            )

            self.maybe_send_processed(
                message.sender,
                processed_message,
            )
        except (InvalidAddress, UnknownAddress, UnknownTokenAddress):
            if is_debug_log_enabled:
                log.warn('Exception while processing message', exc_info=True)
        else:
            if is_debug_log_enabled:
                log.debug(
                    'PROCESSED',
                    node=pex(self.raiden_service.address),
                    to=pex(message.sender),
                    echohash=pex(echohash),
                )

    def maybe_send_processed(self, receiver_address, message):
        if not isinstance(message, Processed):
            raise TypeError('Only use this method for `Processed` messages')

        message_data = json.dumps(message.to_dict())

        echohash = sha3(message_data.encode() + self.raiden_service.address)
        self._receivedhashes_to_processedmessages[echohash] = (
            receiver_address, message_data
        )

        if self.client.should_listen:
            self._send_immediate(receiver_address, message_data)

    def send_async(self, receiver_address: typing.Address, message: 'Message') -> AsyncResult:
        if not isaddress(receiver_address):
            raise ValueError('Invalid address {}'.format(pex(receiver_address)))

        if isinstance(message, (Processed, Ping)):
            raise ValueError('Do not use send for `Processed` or `Ping` messages')

        if isinstance(message, SignedMessage) and not message.sender:
            # FIXME: This can't be right
            message.sender = self.client.user_id

        # Messages that are not unique per receiver can result in hash
        # collision, e.g. Secret messages. The hash collision has the undesired
        # effect of aborting message resubmission once /one/ of the nodes
        # replied with an Ack, adding the receiver address into the echohash to
        # avoid these collisions.
        data = json.dumps(message.to_dict())
        echohash = sha3(data.encode() + receiver_address)

        # Ignore duplicated messages
        if echohash not in self._senthashes_to_states:
            async_result = AsyncResult()
            self._senthashes_to_states[echohash] = SentMessageState(
                async_result,
                receiver_address,
            )
            self._send_immediate(receiver_address, data)

        else:
            async_result = self._senthashes_to_states[echohash].async_result

        return async_result

    def _send_immediate(self, receiver_address, data):
        room = self._get_room_for_address(receiver_address)
        log.debug('_SEND: %r => %r', room, data)
        room.send_text(data)

    def stop_and_wait(self):
        self.client.set_presence_state(UserPresence.OFFLINE.value)
        self.client.stop_listener_thread()
        self.client.logout()

        # Set all the pending results to False
        for wait_processed in self._senthashes_to_states.values():
            wait_processed.async_result.set(False)

    def _get_room_for_address(self, receiver_address: typing.Address) -> Room:
        room_id = self._address_to_roomid_cache.get(receiver_address)
        if room_id:
            room = self.client.rooms.get(room_id)
            if room:
                return room
            else:
                # Room is gone - remove from cache
                self._address_to_roomid_cache.pop(receiver_address)

        room_name = self._room_sep.join(
            [self._room_prefix, self.network_name] +
            [
                address_encoder(addr).lower()
                for addr in sorted((receiver_address, self.raiden.address))
            ]
        )  # e.g.: raiden_ropsten_0xaaaa_0xbbbb
        for room_id, _room in self.client.get_rooms().items():
            # search for a room with given name
            self._ensure_room_alias(_room)
            if not _room.canonical_alias:
                continue
            if room_name in _room.canonical_alias:
                room = _room
                break
        else:  # no room with expected name => create one and invite peer
            address = address_encoder(receiver_address)
            candidates = self.client.search_user_directory(address)

            if not candidates:
                raise ValueError('No candidates found for given address: {}'.format(address))

            # filter candidates
            peers = [user for user in candidates if _validate_userid_signature(user)]
            if not peers:
                raise ValueError('No valid peer found for given address: {}'.format(address))

            room = self.client.create_room(
                room_name,
                invitees=[user.user_id for user in peers],
                is_public=True  # FIXME: This is for debugging purposes only
            )
            offline_peers = [
                user for user in peers
                if self._userid_to_presence.get(user.user_id) is UserPresence.OFFLINE
            ]
            if offline_peers:
                log.warning('Inviting offline peers', offline_peers=offline_peers, room=room)

        self._address_to_roomid_cache[receiver_address] = room.room_id

        return room

    def _make_room_alias(self, *parts):
        return self._room_sep.join([self._room_prefix, self.network_name, *parts])

    @staticmethod
    def _ensure_room_alias(room):
        if not room.canonical_alias:
            room.update_aliases()
            if room.aliases:
                room.canonical_alias = room.aliases[0]

    def _handle_presence_change(self, event):
        if event['type'] != 'm.presence':
            return
        user_id = event['sender']
        state = UserPresence(event['content']['presence'])
        if state == self._userid_to_presence.get(user_id):
            return
        log.debug(
            'Changing presence state',
            user_id=user_id,
            prev_state=self._userid_to_presence.get(user_id),
            state=state
        )
        self._userid_to_presence[user_id] = state
        # User should be re-validated after presence change
        self._userids_to_address.pop(user_id, None)
        try:
            state_change = ActionChangeNodeNetworkState(
                # FIXME: This should probably use ecrecover instead
                self._address_from_user_id(user_id),
                NODE_NETWORK_UNREACHABLE
                if state is UserPresence.OFFLINE
                else NODE_NETWORK_REACHABLE
            )
        except (binascii.Error, AssertionError):
            # Malformed address - skip
            log.debug('Malfromed address, probably not a raiden node', user_id=user_id)
            return
        self.raiden.handle_state_change(state_change)

    @staticmethod
    def _address_from_user_id(user_id):
        return address_decoder(user_id.partition(':')[0].replace('@', ''))

    def _select_server(self):
        server = self.raiden_service.config['matrix']['server']
        if server.startswith('http'):
            return server
        elif server != 'auto':
            raise ValueError('Invalid matrix server specified (valid values: "auto" or a URL)')

        def _get_rtt(server_name):
            return server_name, get_http_rtt(server_name)

        get_rtt_jobs = [
            gevent.spawn(_get_rtt, server_name)
            for server_name
            in self.raiden_service.config['matrix']['available_servers']
        ]
        gevent.joinall(get_rtt_jobs)
        sorted_servers = sorted((job.value for job in get_rtt_jobs), key=itemgetter(1))
        log.debug('Matrix homeserver RTT times', rtt_times=sorted_servers)
        best_server, rtt = sorted_servers[0]
        log.info(
            'Automatically selecting matrix homeserver based on RTT',
            homeserver=best_server,
            rtt=rtt
        )
        return best_server


def _validate_userid_signature(user: User) -> bool:
    # display_name should be an address present in the user_id
    recovered = signing.recover(
        user.user_id.encode(),
        signature=data_decoder(user.get_display_name()),
        hasher=eth_sign_sha3
    )
    return address_encoder(recovered).lower() in user.user_id

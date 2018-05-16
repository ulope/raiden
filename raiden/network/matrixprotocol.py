import binascii
import json
import logging
import re
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

    # noinspection PyUnresolvedReferences
    def __init__(self, raiden_service: 'RaidenService'):
        self._raiden_service: 'RaidenService' = raiden_service
        self._server_url: str = self._select_server()
        self._server_name = urlparse(self._server_url).hostname
        client_class = self._raiden_service.config['matrix'].get('client_class', GMatrixClient)
        self._client: GMatrixClient = client_class(self._server_url)

        self._discovery_room: Room = None

        self._senthashes_to_states = dict()
        self._receivedhashes_to_processedmessages = dict()
        self._addresses_of_interest: Set[typing.Address] = set()
        self._address_to_userids: Dict[typing.Address, Set[str]] = dict()
        self._userid_to_presence: Dict[str, UserPresence] = dict()
        self._address_to_presence: Dict[typing.Address, UserPresence] = dict()
        self._userids_to_address: Dict[str, typing.Address] = dict()
        self._address_to_roomid: Dict[typing.Address, str] = dict()

        discovery_cfg = self._raiden_service.config['matrix']['discovery_room']
        self._discovery_room_alias = self._make_room_alias(discovery_cfg['alias_fragment'])
        self._discovery_room_alias_full = (
            f'#{self._discovery_room_alias}:{discovery_cfg["server"]}'
        )
        room_alias_re = self._make_room_alias(
            '(?P<peer1>0x[a-zA-Z0-9]{40})',
            '(?P<peer2>0x[a-zA-Z0-9]{40})'
        )
        self._room_alias_re = re.compile(f'^#{room_alias_re}:(?P<server_name>.*)$')

    def start(self):
        self._login_or_register()
        self._inventory_rooms()

        self._client.add_invite_listener(self._handle_invite)
        self._client.add_presence_listener(self._handle_presence_change)
        # TODO: Add (better) error handling strategy
        self._client.start_listener_thread(exception_handler=lambda e: None)
        # TODO: Add greenlet that regularly refreshes our presence state
        self._client.set_presence_state(UserPresence.ONLINE.value)

        # Important: Join the discovery room last to ensure we can react to invites
        self._join_discovery_room()
        self._discovery_room.add_listener(self._handle_discovery_membership_event, 'm.room.member')

        gevent.spawn_later(5, self._ensure_room_peers)
        log.info('TRANSPORT STARTED')

    def start_health_check(self, node_address):
        node_address_hex = address_encoder(node_address)
        log.DEV('Healthcheck', address=node_address_hex)
        users = [
            user
            for user
            in self._client.search_user_directory(address_encoder(node_address))
            if _validate_userid_signature(user)
        ]
        log.DEV('found users', users=users)
        existing = {presence['user_id'] for presence in self._client.get_presence_list()}
        user_ids_to_add = {u.user_id for u in users}
        user_ids = user_ids_to_add - existing
        if user_ids:
            log.debug('Add to presence list', added_users=user_ids)
            self._client.modify_presence_list(add_user_ids=list(user_ids))
        self._address_to_userids.setdefault(node_address, set()).update(user_ids_to_add)
        log.DEV('_address_to_userids', content=self._address_to_userids,
                user_ids_to_add=user_ids_to_add)
        # Ensure there is a room for the peer node
        # We use spawn_later to avoid races if the peer is already expecting us and sent an invite
        gevent.spawn_later(5, self._get_room_for_address, node_address, allow_missing_peers=True)

    # noinspection PyUnresolvedReferences
    def send_async(self, receiver_address: typing.Address, message: 'Message') -> AsyncResult:
        if not isaddress(receiver_address):
            raise ValueError('Invalid address {}'.format(pex(receiver_address)))

        if isinstance(message, (Processed, Ping)):
            raise ValueError('Do not use send for `Processed` or `Ping` messages')

        if isinstance(message, SignedMessage) and not message.sender:
            # FIXME: This can't be right
            message.sender = self._client.user_id

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

    def stop_and_wait(self):
        self._client.set_presence_state(UserPresence.OFFLINE.value)
        self._client.stop_listener_thread()
        self._client.logout()

        # Set all the pending results to False
        for wait_processed in self._senthashes_to_states.values():
            wait_processed.async_result.set(False)

    @property
    def _network_name(self) -> str:
        return ID_TO_NETWORKNAME.get(
            self._raiden_service.network_id,
            str(self._raiden_service.network_id)
        )

    def _login_or_register(self):
        # password is signed server address
        password = data_encoder(self._sign(self._server_url.encode()))
        seed = int.from_bytes(self._sign(b'seed')[-32:], 'big')
        rand = Random()  # deterministic, random secret for username suffixes
        rand.seed(seed)
        # try login and register on first 5 possible accounts
        for i in range(5):
            base_username = address_encoder(self._raiden_service.address)
            username = base_username
            if i:
                username = f'{username}.{rand.randint(0, 0xffffffff):08x}'

            try:
                self._client.login_with_password(username, password)
                log.info(
                    'LOGIN',
                    homeserver=self._server_url,
                    username=username
                )
                break
            except MatrixRequestError as ex:
                if ex.code != 403:
                    raise
                log.debug(
                    'Could not login. Trying register',
                    homeserver=self._server_url,
                    username=username,
                )
                try:
                    self._client.register_with_password(username, password)
                    log.info(
                        'REGISTER',
                        homeserver=self._server_url,
                        username=username,
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
        name = data_encoder(self._sign(self._client.user_id.encode()))
        self._client.get_user(self._client.user_id).set_display_name(name)

    def _join_discovery_room(self):
        discovery_cfg = self._raiden_service.config['matrix']['discovery_room']
        try:
            discovery_room = self._client.join_room(self._discovery_room_alias_full)
        except MatrixRequestError as ex:
            if ex.code != 404:
                raise
            # Room doesn't exist
            if discovery_cfg['server'] != self._server_name:
                raise RuntimeError(
                    f"Discovery room {self._discovery_room_alias_full} not found and can't be "
                    f"created on a federated homeserver."
                )
            discovery_room = self._client.create_room(self._discovery_room_alias, is_public=True)
        self._discovery_room = discovery_room
        # Populate initial members
        self._discovery_room.get_joined_members()

    def _inventory_rooms(self):
        # Iterate over a copy so we can modify the room list
        for room_id, room in list(self._client.rooms.items()):
            if not room.canonical_alias:
                # Leave any rooms that don't have a canonical alias as they are not part of the
                # protocol
                room.leave()
                continue
            # Don't listen for messages in the discovery room
            should_listen = room.canonical_alias != self._discovery_room_alias_full
            if should_listen:
                peer_address = self._get_peer_address_from_room(room.canonical_alias)
                if not peer_address:
                    log.warning(
                        'Member of room we\'re not supposed to be a member of - ignoring',
                        room=room
                    )
                    return
                self._address_to_roomid[peer_address] = room.room_id
                room.add_listener(self._handle_message)
            log.debug(
                'ROOM',
                room_id=room_id,
                aliases=room.aliases,
                listening=should_listen
            )

    def _handle_invite(self, room_id: str, state: dict):
        """ Join all invited rooms """
        room = self._client.join_room(room_id)
        if not room.canonical_alias:
            log.warning('Got invited to room without canonical alias - ignoring', room=room)
            return
        peer_address = self._get_peer_address_from_room(room.canonical_alias)
        if not peer_address:
            log.warning('Got invited to room we\'re not a member of - ignoring', room=room)
            return
        self._address_to_roomid[peer_address] = room.room_id
        room.add_listener(self._handle_message, 'm.room.message')
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

        log.DEV('MESSAGE', sender=sender_id)

        if sender_id == self._client.user_id:
            # Ignore our own messages
            return

        user = self._client.get_user(sender_id)

        node_address = self._userids_to_address.get(sender_id)
        if not node_address:
            try:
                # recover displayname signature
                node_address = signing.recover(
                    sender_id.encode(),
                    signature=data_decoder(user.get_display_name()),
                    hasher=eth_sign_sha3
                )
            except AssertionError:
                log.warning('INVALID MESSAGE', sender_id=sender_id)
                return
            node_address_hex = address_encoder(node_address)
            if node_address_hex.lower() not in sender_id:
                log.warning(
                    'INVALID SIGNATURE',
                    peer_address=node_address_hex,
                    sender_id=sender_id
                )
                return
            self._userids_to_address[sender_id] = node_address

        data = event['content']['body']
        if data.startswith('0x'):
            message = message_from_bytes(data_decoder(data))
        else:
            message_dict = json.loads(data)
            log.trace('MESSAGE_DATA', data=message_dict)
            message = message_from_dict(message_dict)

        if isinstance(message, SignedMessage) and not message.sender:
            # FIXME: This can't be right
            message.sender = node_address

        echohash = sha3(data.encode() + self._raiden_service.address)
        if echohash in self._receivedhashes_to_processedmessages:
            self._maybe_send_processed(*self._receivedhashes_to_processedmessages[echohash])
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
        waitprocessed = self._senthashes_to_states.pop(
            processed.processed_message_identifier,
            None
        )

        if waitprocessed is None:
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    '`Processed` MESSAGE UNKNOWN ECHO',
                    node=pex(self._raiden_service.address),
                    message_identifier=pex(processed.processed_message_identifier),
                )

        else:
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    '`Processed` MESSAGE RECEIVED',
                    node=pex(self._raiden_service.address),
                    receiver=pex(waitprocessed.receiver_address),
                    message_identifier=pex(processed.processed_message_identifier),
                )

            waitprocessed.async_result.set(True)

    def _receive_message(self, message, echohash):
        is_debug_log_enabled = log.isEnabledFor(logging.DEBUG)

        if is_debug_log_enabled:
            log.info(
                'MESSAGE RECEIVED',
                node=pex(self._raiden_service.address),
                echohash=pex(echohash),
                message=message,
                message_sender=pex(message.sender)
            )

        try:
            on_udp_message(self._raiden_service, message)

            # only send the Processed message if the message was handled without exceptions
            processed_message = Processed(
                self._raiden_service.address,
                echohash,
            )

            self._maybe_send_processed(
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
                    node=pex(self._raiden_service.address),
                    to=pex(message.sender),
                    echohash=pex(echohash),
                )

    def _maybe_send_processed(self, receiver_address, message):
        if not isinstance(message, Processed):
            raise TypeError('Only use this method for `Processed` messages')

        message_data = json.dumps(message.to_dict())

        echohash = sha3(message_data.encode() + self._raiden_service.address)
        self._receivedhashes_to_processedmessages[echohash] = (
            receiver_address, message_data
        )

        if self._client.should_listen:
            self._send_immediate(receiver_address, message_data)

    def _send_immediate(self, receiver_address, data):
        # FIXME: Use all matching rooms
        room = self._get_room_for_address(receiver_address)
        log.debug('_SEND: %r => %r', room, data)
        room.send_text(data)

    def _leave_rooms(self):
        log.DEV('Leaving all rooms')
        for room in list(self._client.rooms.values()):
            leave_result = room.leave()
            log.trace('Leaving', room=room, result=leave_result)
        log.DEV('Left all rooms', rooms=self._client.rooms)

    def _get_room_for_address(
        self,
        receiver_address: typing.Address,
        allow_missing_peers=False
    ) -> Room:
        room_id = self._address_to_roomid.get(receiver_address)
        if room_id:
            room = self._client.rooms.get(room_id)
            if room:
                return room
            else:
                # Room is gone - remove from cache
                self._address_to_roomid.pop(receiver_address)

        # The addresses are being sorted to ensure the same channel is used for both directions
        # of communication.
        # e.g.: raiden_ropsten_0xaaaa_0xbbbb
        address_pair = sorted([
            address_encoder(address).lower()
            for address in [receiver_address, self._raiden_service.address]
        ])
        room_name = self._make_room_alias(*address_pair)

        room_candidates = self._client.search_room_directory(room_name)
        if room_candidates:
            room = room_candidates[0]
        else:
            # no room with expected name => create one and invite peer
            address = address_encoder(receiver_address)
            candidates = self._client.search_user_directory(address)
            if not candidates and not allow_missing_peers:
                raise ValueError('No candidates found for given address: {}'.format(address))

            # filter candidates
            peers = [user for user in candidates if _validate_userid_signature(user)]
            if not peers and not allow_missing_peers:
                raise ValueError('No valid peer found for given address: {}'.format(address))

            try:
                # Try join first to avoid races
                room_name_full = f'#{room_name}:{self._server_name}'
                log.DEV('Trying to join', room_name=room_name_full)
                room = self._client.join_room(room_name_full)
                for user in peers:
                    room.invite_user(user.user_id)
            except MatrixRequestError:
                room = self._client.create_room(
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

        room.add_listener(self._handle_message, 'm.room.message')
        log.info('CHANNEL ROOM', peer_address=address_encoder(receiver_address), room=room)
        self._address_to_roomid[receiver_address] = room.room_id
        return room

    def _make_room_alias(self, *parts):
        return self._room_sep.join([self._room_prefix, self._network_name, *parts])

    def _handle_presence_change(self, event):
        """
        Update node network reachability from presence events.

        Due to the possibility of nodes using accounts on multiple homeservers a composite
        address state is synthesised from the cached individual user presence state.
        """
        if event['type'] != 'm.presence':
            return
        user_id = event['sender']
        new_state = UserPresence(event['content']['presence'])
        if new_state == self._userid_to_presence.get(user_id):
            return

        log.trace(
            'Changing user presence state',
            user_id=user_id,
            prev_state=self._userid_to_presence.get(user_id),
            state=new_state
        )
        self._userid_to_presence[user_id] = new_state

        # User should be re-validated after presence change
        self._userids_to_address.pop(user_id, None)

        try:
            # FIXME: This should probably use ecrecover instead
            address = self._address_from_user_id(user_id)
        except (binascii.Error, AssertionError):
            # Malformed address - skip
            log.debug('Malformed address, probably not a raiden node', user_id=user_id)
            return

        composite_presence = {
            self._userid_to_presence.get(uid)
            for uid
            in self._address_to_userids.get(address, set())
        }

        # Iterate over UserPresence in definition order and pick first matching state
        new_state = UserPresence.OFFLINE
        for presence in UserPresence.__members__.values():
            if presence in composite_presence:
                new_state = presence
                break

        if new_state == self._address_to_presence.get(address):
            return

        log.debug(
            'Changing address presence state',
            address=address_encoder(address),
            user_id=user_id,
            prev_state=self._address_to_presence.get(address),
            state=new_state
        )
        self._address_to_presence[address] = new_state

        state_change = ActionChangeNodeNetworkState(
            address,
            NODE_NETWORK_UNREACHABLE
            if new_state is UserPresence.OFFLINE
            else NODE_NETWORK_REACHABLE
        )
        self._raiden_service.handle_state_change(state_change)

    def _handle_discovery_membership_event(self, room, event):
        if event['type'] != 'm.room.member':
            return

        state = event['content']['membership']
        user_id = event['state_key']
        log.DEV('discovery member change', state=state, user_id=user_id)
        if state != 'join':
            return
        self._maybe_invite_user(self._client.get_user(user_id))

    def _ensure_room_peers(self):
        """ Check all members of discovery channel for matches to existing rooms """
        for member in self._discovery_room.get_joined_members():
            self._maybe_invite_user(member)

    def _maybe_invite_user(self, user):
        try:
            address = self._address_from_user_id(user.user_id)
        except (binascii.Error, AssertionError):
            return

        room_id = self._address_to_roomid.get(address)
        if not room_id:
            return

        # This is an address we care about - add new user to health check
        if user.user_id not in self._address_to_userids.get(address, set()):
            self.start_health_check(address)

        # Health check will ensure room exists
        room = self._client.rooms.get(room_id)
        # Refresh members
        room.get_joined_members()
        if user.user_id not in room._members.keys():
            log.DEV('INVITE', user=user)
            room.invite_user(user.user_id)

    @staticmethod
    def _address_from_user_id(user_id):
        return address_decoder(user_id.partition(':')[0].replace('@', '').partition('.')[0])

    def _select_server(self):
        server = self._raiden_service.config['matrix']['server']
        if server.startswith('http'):
            return server
        elif server != 'auto':
            raise ValueError('Invalid matrix server specified (valid values: "auto" or a URL)')

        def _get_rtt(server_name):
            return server_name, get_http_rtt(server_name)

        get_rtt_jobs = [
            gevent.spawn(_get_rtt, server_name)
            for server_name
            in self._raiden_service.config['matrix']['available_servers']
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

    def _sign(self, data: bytes) -> bytes:
        """Use eth_sign compatible hasher to sign matrix data"""
        return signing.sign(
            data,
            self._raiden_service.private_key,
            hasher=eth_sign_sha3
        )

    def _get_peer_address_from_room(self, room_alias):
        match = self._room_alias_re.match(room_alias)
        if match:
            addresses = {address_decoder(address) for address in (match.group('peer1', 'peer2'))}
            addresses = addresses - {self._raiden_service.address}
            if len(addresses) == 1:
                return addresses.pop()


def _validate_userid_signature(user: User) -> bool:
    # display_name should be an address present in the user_id
    recovered = signing.recover(
        user.user_id.encode(),
        signature=data_decoder(user.get_display_name()),
        hasher=eth_sign_sha3
    )
    return address_encoder(recovered).lower() in user.user_id

/*
This is a token contract based on HumanStandardToken that allows anyone to mint
new tokens on demand. Useful for testing and debugging.
*/

pragma solidity ^0.4.11;
import './HumanStandardToken.sol';


contract InfiniToken is HumanStandardToken {
    function InfiniToken(
        string _tokenName,
        string _tokenSymbol
    ) HumanStandardToken(
        0,
        _tokenName,
        8,
        _tokenSymbol
    ) {
    }

    function mint() {
        mint(100);
    }

    function mint(uint amount) {
        mintFor(amount, msg.sender);
    }

    function mintFor(uint amount, address target) {
        _total_supply += amount;
        balances[target] += amount;
    }
}

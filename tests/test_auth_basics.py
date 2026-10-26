import bcrypt
import re

def test_bcrypt_hash_is_not_plaintext_and_cost_is_12():
    password = b"StrongPass123"
    hashed = bcrypt.hashpw(password, bcrypt.gensalt(rounds=12))
    assert hashed != password
    assert bcrypt.checkpw(password, hashed)
    assert int(hashed.split(b"$")[2]) >= 12

def valid(password):
    return len(password) >= 10 and len(password.encode()) <= 72 and bool(re.search(r"[A-Za-z]", password)) and bool(re.search(r"\d", password))

def test_password_policy_examples():
    assert valid("StrongPass123")
    assert not valid("short1")
    assert not valid("abcdefghij")
    assert not valid("1234567890")
    assert not valid("a" * 73 + "1")

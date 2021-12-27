from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
private_key = Ed25519PrivateKey.generate()
signature = private_key.sign(b"my authenticated message")
public_key = private_key.public_key()
public_key.public_bytes()
# Raises InvalidSignature if verification fails
public_key.verify(signature, b"my authenticated message")

pass
print(2+2)



import base64
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

password = b"password"

salt = os.urandom(16)
kdf = PBKDF2HMAC(
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,
    iterations=390000,
)
key = base64.urlsafe_b64encode(kdf.derive(password))
f = Fernet(key)
token = f.encrypt(b"Secret message!")
d = f.decrypt(token)
print(d)

kdf2 = PBKDF2HMAC( algorithm=hashes.SHA256(), length=32, salt=salt, iterations=390000, )
key2 = base64.urlsafe_b64encode(kdf2.derive(password))
f2 = Fernet(key2)
token2 = f2.encrypt(b"Secret message!")
d2 = f2.decrypt(token2)
print(d2)

pass



# import nacl.utils
# from nacl.public import PrivateKey, Box
#
# skbob = PrivateKey.generate()
# pkbob = skbob.public_key
#
# skalice = PrivateKey.generate()
# pkalice = skalice.public_key
#
# bob_box = Box(skbob, pkalice)
#
# message = b"Kill all humans"
#
# nonce = nacl.utils.random(Box.NONCE_SIZE)
# encrypted = bob_box.encrypt(message, nonce)
#
# alice_box = Box(skalice, pkbob)
# plaintext = alice_box.decrypt(encrypted)
# print(plaintext.decode('utf-8'))
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
from rsocket.payload import Payload
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from Cryptodome.Cipher import PKCS1_OAEP
from Cryptodome.Hash import SHA256, SHA1
from Cryptodome.Signature import pss
import logging
import snappy


def decode_public_key(bytes_data):
    encoded_key = bytes_data.decode("utf-8")
    if encoded_key == 'plaintext':
        logging.info("Plaintext is being used")
        return None

    cert = load_pem_x509_certificate(bytes_data, default_backend())

    public_key = cert.public_key()
    logging.info(f"Decoded public key: {public_key}")
    return public_key


def encrypt_payload(data, public_key):
    if public_key is None:
        return Payload(data, 'plaintext')

    private_key = Fernet.generate_key()
    data_encryptor = Fernet(private_key)
    key_encryptor = PKCS1_OAEP.new(key=private_key, hashAlgo=SHA256, mgfunc=lambda x, y: pss.MGF1(x, y, SHA1))

    encrypted_data = data_encryptor.encrypt(bytes(snappy.compress(data), 'utf-8'))
    encrypted_key = key_encryptor.encrypt(public_key.public_bytes(encoding=serialization.Encoding.PEM))
    logging.info(f"Returning: Encrypted Data - {encrypted_data}\nEncrypted Key - {encrypted_key}")
    return Payload(encrypted_data, encrypted_key)

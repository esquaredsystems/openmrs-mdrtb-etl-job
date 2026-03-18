from dotenv import load_dotenv
import os

load_dotenv()

BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000))

from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
def health_check():
    return 'Bot is running', 200

def start_server():
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

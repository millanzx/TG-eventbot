import os
import logging
import asyncio
from flask import Flask, request, jsonify
from telegram import Update
from Zapis2 import setup_bot

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize the bot application
# This starts background threads (Google Sheets, Reminders) defined in Zapis2.py
bot_app = setup_bot()

if bot_app is None:
    logger.error("Failed to initialize bot application! Check environment variables and configuration.")

@app.route('/webhook', methods=['POST'])
async def webhook():
    """
    Webhook endpoint to receive updates from Telegram.
    """
    if request.method == 'POST':
        try:
            if not bot_app:
                return jsonify({'status': 'error', 'message': 'Bot failed to initialize'}), 500

            # Ensure application is initialized (lazy init for async parts)
            # Application.initialize() and start() are async methods needed for PTB v20+
            if not getattr(bot_app, "_initialized", False):
                await bot_app.initialize()
                await bot_app.start()

            data = request.json
            # logger.info(f"Received webhook data: {data}")

            # Create Update object from JSON
            update = Update.de_json(data, bot_app.bot)

            # Process the update
            await bot_app.process_update(update)

            return jsonify({'status': 'ok'}), 200
        except Exception as e:
            logger.error(f"Error processing webhook: {e}", exc_info=True)
            return jsonify({'status': 'error', 'message': str(e)}), 500
    else:
        return jsonify({'status': 'error', 'message': 'Method not allowed'}), 405

@app.route('/', methods=['GET'])
def index():
    status = "running" if bot_app else "failed to initialize"
    return f"Webhook service is {status}!"

if __name__ == '__main__':
    # Get port from environment variable or default to 5000
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

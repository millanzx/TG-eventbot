# Telegram Master-Class Registration Bot

## Overview
This Telegram bot provides a comprehensive system for managing master-class registrations, including user registration, admin management, automated reminders, and Google Sheets integration.

**Recent Updates:**
- Added **Webhook Support**: Run the bot via Flask webhook (`HookZapis.py`) or traditional polling (`Zapis2.py`).
- Refactored `Zapis2.py` to expose a `setup_bot()` function for external initialization.
- Improved start button logic and suppressed duplicate UI hints.

## Key Features

### 👤 User Features

#### Registration System
- **Personal Registration**: Users can register for master-classes for themselves
- **Family Registration**: Users can register family members for master-classes
- **Duplicate Prevention**: System prevents users from registering twice for the same master-class on the same day
- **Rescheduling**: Users can change their master-class or date/time after registration

#### Master-Class Selection
- **Available Classes**: Browse and select from available master-classes
- **Calendar Navigation**: Interactive calendar for date selection
- **Time Slots**: Choose from available time slots for selected dates
- **Weekend Exclusion**: Master-classes can exclude weekends from available dates

#### Registration Management
- **View Registrations**: Check current and past registrations
- **Modify Registrations**: Change master-class, date, or time
- **Delete Registrations**: Cancel registrations if needed
- **Status Tracking**: Real-time status updates (created, confirmed, transferred, cancelled)

### 🔔 Automated Reminders

#### Timing-Based Reminders
- **24-Hour Reminder**: Sent 23.5-24.5 hours before master-class starts
- **60-Minute Reminder**: Sent 45-75 minutes before master-class starts
- **Missed Reminders**: System recovers and sends missed reminders on bot restart

#### Admin Reminders
- **Custom Reminders**: Admins can create custom reminder messages
- **Scheduled Reminders**: Set reminders for specific times/dates
- **Recurring Reminders**: Daily, weekly, or one-time reminders
- **Targeted Messaging**: Send to all users, specific master-class participants, or both
- **Admin Override**: Admins receive all custom reminders regardless of registration status

### 🔐 Admin Panel

#### Master-Class Management
- **Create Master-Classes**: Add new master-classes with full configuration
- **Edit Master-Classes**: Modify name, description, dates, times, capacity
- **Delete Master-Classes**: Remove master-classes with user notifications
- **Weekend Policy**: Enable/disable weekend availability per master-class
- **Capacity Management**: Set total spots and track registrations

#### User Management
- **View Participants**: See all registered users for each master-class
- **Remove Users**: Manually cancel user registrations
- **Filter Users**: Filter by master-class or view all

### 📊 Google Sheets Integration
- **Syncing**: Automatic syncing of registrations to Google Sheets
- **Masking**: Personal data (Surname, Second name) masked for privacy in sheets, First Name remains visible.
- **Admin Reload**: Force reload data from sheets via admin panel

## Deployment Options

### 1. Long Polling (Default)
Run the bot directly using standard long polling:
```bash
python Zapis2.py
```

### 2. Webhook (New)
Run the bot as a Flask application to receive updates via webhook:
```bash
python HookZapis.py
```
*Requires setting up a webhook URL with Telegram API pointing to your server's address.*

## Setup
1. Install requirements: `pip install -r requirements.txt`
2. Set environment variables:
   - `TELEGRAM_BOT_TOKEN`: Your bot token
   - `TELEGRAM_ADMIN_IDS`: Comma-separated admin IDs
   - `ADMIN_PASSWORD`: Password for admin panel
   - `PORT`: (Optional) Port for webhook server (default 5000)
3. Ensure `credentials.json` is present for Google Sheets integration.

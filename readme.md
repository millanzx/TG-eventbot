# Telegram Master-Class Registration Bot

## Overview
This Telegram bot provides a comprehensive system for managing master-class registrations, including user registration, admin management, automated reminders, and Google Sheets integration.

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
- **View Participants**: List all registered users for specific master-classes
- **Remove Users**: Cancel user registrations with notifications
- **Family Management**: Handle family registrations and dependencies
- **Bulk Operations**: Manage multiple registrations efficiently

#### Advanced Features
- **Specific Time Slots**: Create custom time slots for special dates
- **Real-time Updates**: Automatic capacity updates and notifications
- **Audit Logging**: Complete admin action tracking
- **Data Export**: Integration with Google Sheets for reporting

### 📊 Google Sheets Integration

#### Automatic Synchronization
- **Registration Logs**: All user actions logged to Google Sheets
- **Master-Class Data**: Master-class information stored and updated
- **Real-time Updates**: Changes reflected immediately in spreadsheets
- **Backup System**: Automatic data backup and recovery

#### Data Structure
- **Visitors Sheet**: Registration history with user details
- **Master-Classes Sheet**: Master-class configuration and statistics
- **Protected Data**: Sensitive information masked for privacy

### 🕐 Time Zone & Scheduling

#### Moscow Time Zone
- **Consistent Timing**: All operations use Moscow time (UTC+3)
- **Accurate Reminders**: Timezone-aware reminder scheduling
- **Calendar Accuracy**: Date/time calculations in local timezone

#### Background Processing
- **Reminder Threads**: Dedicated threads for reminder processing
- **Queue System**: Prioritized background tasks for Google Sheets
- **Error Recovery**: Automatic retry mechanisms for failed operations

## Technical Architecture

### Database
- **SQLite3**: Local database for registrations and reminders
- **Thread-Safe Operations**: Concurrent access protection
- **Data Integrity**: Foreign key constraints and validation

### External Services
- **Telegram Bot API**: Message handling and user interactions
- **Google Sheets API**: Data storage and reporting
- **Google Service Account**: Secure API authentication

### Security Features
- **Admin Authentication**: Password-protected admin access
- **User Verification**: Telegram verification requirements
- **Data Masking**: Privacy protection in logs and exports
- **Error Handling**: Comprehensive exception management

### Performance Optimizations
- **Asynchronous Operations**: Non-blocking message sending
- **Background Threads**: Separate threads for reminders and Google Sheets
- **Caching System**: Master-class data caching for performance
- **Queue Management**: Prioritized task processing

## Supported Languages
- **Russian**: Full Russian language interface
- **Emoji Integration**: Visual indicators throughout the interface

## Deployment Ready
This bot is designed for cloud deployment on platforms like Render, with proper configuration files for production hosting.


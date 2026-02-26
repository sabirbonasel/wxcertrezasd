import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import sys
from datetime import datetime

stop_script = False  # global flag

def load_lines(filename):
    with open(filename, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def log_result(recipient, status, error=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} | {recipient} | {status}"
    if error:
        log_entry += f" | {error}"
    log_entry += "\n"
    with open("send_results.log", "a") as log_file:
        log_file.write(log_entry)

def send_email(args, recipient, from_name, from_email, subject):
    global stop_script
    if stop_script:
        return False
    try:
        # Build message
        msg = MIMEMultipart()
        msg['From'] = f"{from_name} <{from_email}>"
        msg['To'] = recipient
        msg['Subject'] = subject
        with open(args.html, 'r') as f:
            html_body = f.read()
        msg.attach(MIMEText(html_body, 'html'))

        # Send via SMTP
        with smtplib.SMTP(args.smtp_server, 25) as server:
            server.starttls()
            server.send_message(msg)

        log_result(recipient, "SENT")
        print(f"✅ Sent to {recipient}")
        return True

    except smtplib.SMTPResponseException as e:
        error_msg = f"SMTP error {e.smtp_code}: {e.smtp_error.decode()}"
        log_result(recipient, "BOUNCE", error_msg)
        print(f"✉️ Bounced from {recipient}: {error_msg}")
        if "5.7.1" in error_msg:
            print("❌ Critical error 5.7.1 detected. Stopping script.")
            stop_script = True
        return False

    except Exception as e:
        error_msg = str(e)
        log_result(recipient, "FAILED", error_msg)
        print(f"⚠️ Failed to send to {recipient}: {error_msg}")
        return False

def run_bulk(args, recipients, from_names, subjects, from_email):
    global stop_script
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = []
        for recipient in recipients:
            from_name = random.choice(from_names)
            subject = random.choice(subjects)
            futures.append(executor.submit(
                send_email,
                args,
                recipient,
                from_name,
                from_email,
                subject
            ))
            time.sleep(0.1)

        for future in as_completed(futures):
            if stop_script:
                print("🛑 Stopping all tasks due to critical error (5.7.1)")
                executor.shutdown(wait=False, cancel_futures=True)
                sys.exit(1)
            future.result()

def main():
    parser = argparse.ArgumentParser(description="Bulk email sender with optional test-only mode")
    parser.add_argument('--recipients', required=True, help='Path to recipients file')
    parser.add_argument('--html', required=True, help='Path to HTML email template')
    parser.add_argument('--froms', required=True, help='Path to from names file')
    parser.add_argument('--subjects', required=True, help='Path to subject lines file')
    parser.add_argument('--smtp-server', required=True, help='SMTP server address')
    parser.add_argument('--threads', type=int, default=150, help='Number of threads (default 30)')
    parser.add_argument('--test-only', action='store_true', help='Send only first 2 emails and stop')
    args = parser.parse_args()

    # Init log
    with open("send_results.log", "w") as log_file:
        log_file.write("TIMESTAMP | RECIPIENT | STATUS | ERROR (if any)\n")
        log_file.write("="*80 + "\n")

    # Load data
    for f in [args.recipients, args.html, args.froms, args.subjects]:
        if not os.path.exists(f):
            print(f"❌ File not found: {f}")
            sys.exit(1)

    recipients = load_lines(args.recipients)
    from_names = load_lines(args.froms)
    subjects = load_lines(args.subjects)
    if not recipients or not from_names or not subjects:
        print("❌ One or more input files are empty")
        sys.exit(1)

    # Prepare From email
    random_id = random.randint(100, 999)
    from_email = f"rewards-{random_id}@blogster.com"

    print(f"📧 From: {random.choice(from_names)} <{from_email}>")
    print(f"📨 Total recipients: {len(recipients)}")
    print(f"🧵 Using {args.threads} threads")

    # STEP 1: Send first 2 emails
    print("\n🧪 Sending test emails (2 recipients)...")
    test_recipients = recipients[:2]
    run_bulk(args, test_recipients, from_names, subjects, from_email)

    if stop_script:
        sys.exit(1)

    if args.test_only:
        print("\n✅ Test-only mode finished. No bulk sending.")
        sys.exit(0)

    # STEP 2: Bulk sending
    print("\n🚀 Test successful. Continuing with bulk send...")
    run_bulk(args, recipients[2:], from_names, subjects, from_email)

    print("\n🎉 Bulk sending completed!")
    print("Results saved to send_results.log")

if __name__ == "__main__":
    main()
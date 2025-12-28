import glob 

# Function to get the path of the mailbox file
def _mailbox_path():
    mailbox_path = glob.glob('data/*.mbox')
    return mailbox_path

if __name__ == '__main__':
    print(mailbox_path())
import s3_to_lake

def sync_s3_to_lake():
    inbound_handler = s3_to_lake.S3_To_Lake()
    inbound_handler.process()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Start')
    sync_s3_to_lake()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

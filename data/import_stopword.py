import argparse, predictionio, uuid

def import_events(client, file):
    count = 0
    print('============================')
    print("Importing stopwords...")
    print('============================')
    with open(file) as f:
        content = f.readlines()
        # you may also want to remove whitespace characters like `\n` at the end of each line
        for stopword in content:
            word = stopword.replace('\n', '')
            print("Inserting %s..." % word)
            client.create_event(
                event="stopwords",
                entity_type="resource",
                entity_id=str(count),
                # use the count num as user ID
                properties={
                    "word": word
                }
            )
            count += 1
        f.close()
    print("%s stopwords are imported." % count)
    print('============================')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import sample data for classification engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--file', default="./stopwords_en.txt")
    parser.add_argument('--test', default=True)

    args = parser.parse_args()
    print(args)

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500)
    import_events(client, args.file)

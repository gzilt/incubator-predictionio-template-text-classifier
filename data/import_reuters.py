import os, json, argparse, predictionio, re, uuid

from html.parser import HTMLParser


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def import_events(client, path):
    print('=====================================')
    print("Importing data default training data ")
    print('=====================================')
    files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
    count = 0
    map = []
    for file in files:
        print("Importing data from file: " + file)
        with open(os.path.join(path, file)) as json_file:
            data = list(json.load(json_file))
            for item in data:
                if item['attrs']['topics'] == 'YES':
                    if len(item['topics']) > 1:
                        for tag in item['topics']:

                            if tag not in map:
                                map.append(tag)

                            client.create_event(
                                event="documents",
                                entity_type="source",
                                entity_id=str(count),
                                # use the count num as user ID
                                properties={
                                    "text": re.sub(' +', ' ',
                                                   strip_tags(item['body']).replace('"', '').replace('\n', ' ')),
                                    "category": tag,
                                    "label": int(map.index(tag))
                                }
                            )
                            count += 1
            json_file.close()

    print(map)

    print('=========================')
    print("%s events are imported." % count)
    print('=========================')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import sample data for classification engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--path', default="./reuters-21578")

    args = parser.parse_args()
    print(args)

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500)
    import_events(client, args.path)

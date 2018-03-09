import re, os, argparse, predictionio, string, csv, codecs
# import xml.etree.cElementTree as ET
from html.parser import HTMLParser
# import lxml
from lxml.html.clean import Cleaner
from string import punctuation


def import_events(client, path, is_test):
    cleaner = Cleaner()
    cleaner.javascript = True  # This is True because we want to activate the javascript filter
    cleaner.style = True
    cleaner.scripts = True
    cleaner.comments = True
    cleaner.inline_style = True
    cleaner.kill_tags = ['a', 'sup', 'img']
    tempListCanonical = []
    tempListMetaTitle = []
    iabMap = {}
    runReport = {}
    data = {}
    reps = {
        r'<[^>]+>': '',
        '&nbsp;': ' ',
        '///': '',
        '’': '\'',
        '`': '\'',
        '"': '',
        '–': '-',
        r'\u2014': '-',
        r'\u2026': '',
        r'\u2022': ' ',
        r'\u0022': '',
        'Advertisement - Continue Reading Below': '',
        'Advertisement': '',
        'ADVERTISEMENT': '',
        'https?:\/\/.*[\r\n]*': '',
        'pic.twitter.com.*[\r\n]*': '',
        '\[image.+image]': '',
        '\[soundcloud.+\]': '',
        # dataset specifics
        '\(Photo.+\)': '',
        'Credit:.+Getty Images': '',
        'Getty Images': '',

        # https://www.autodeal.com.ph
        r'(?s)(Related Articles.*?)(?:(?:\r*\n){}|~end~)': '',
        r'(?s)(Get Quote.*?)(?:(?:\r*\n){}|Get Quote)': '',
        r'(?m)Related:.*\n?': '',
        r'(?m)Review:.*\n?': '',

        # 'Related Articles.+Latest Review': '',
        # 'Get Quote.+Get Quote': '',
        # '/ Review': '',
        # 'Jacob Oliva': '',
        # ' Review:': '',
        # '·': '',
        # 'Jan 12, 2018': '',
        # 'Jan 19, 2018': '',
        # '/ 5': ' out of 5',
        # cleanup white spaces
        r'\s+': ' '
    }
    wordCounts = dict()
    wordCountsTop = dict()

    print('============================')
    print("Importing xml content...")
    print('============================')

    with open("lookup_iab.csv", 'r+', encoding='UTF-8') as iab:
        reader = csv.reader(iab, delimiter=',')
        for item in reader:
            iabMap[item[1]] = item[0]

    files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.txt')]

    for file in files:
        count = 0
        noTitle = 0
        duplicateCount = 0
        contentTooShort = 0
        errorCount = 0
        categoryArr = file.split('_')
        categoryTemp = string.capwords(categoryArr[1])
        categoryIndex = re.sub('&', 'and', string.capwords(categoryArr[1]))
        categoryId = iabMap.get(categoryIndex)

        print("Importing data from file: {} under {}-{}".format(file, categoryId, categoryIndex))

        with open(path + '/' + file, 'r+', encoding='UTF-8') as f:
            content = f.readlines()
            # you may also want to remove whitespace characters like `\n` at the end of each line
            for text in content:
                if not is_test:
                    client.create_event(
                        event="documents",
                        entity_type="source",
                        entity_id=str(count),
                        properties={
                            "text": text,
                            "category": categoryIndex,
                            "label": int(categoryId)
                        }
                    )
                print(
                    '--------------------------------------------------------------------------------------------------------------')
                print(text)
                # words = strip_punctuation(text).split()
                # for word in words:
                #     if word in wordCounts:
                #         wordCounts[word] += 1
                #     else:
                #         wordCounts[word] = 1
                count += 1
            f.close()

        print()
        print('****************************')
        print('* SUMMARY                  *')
        print('****************************')
        print("%s content are imported." % count)
        # print("%s duplicates found." % duplicateCount)
        # print("%s no title." % noTitle)
        # print("%s content too short." % contentTooShort)
        # print("%s error." % errorCount)
        print('****************************')
        if (categoryIndex in runReport):
            runReport[categoryIndex] += count
        else:
            runReport[categoryIndex] = count

    # for word, count in wordCounts.items():
    #     if int(count) > 50:
    #         print("{} = {}".format(word, count))

    # print(wordCounts)
    print(runReport)


# define our method
def replace_all(text, dic):
    for i, j in dic.items():
        text = re.sub(i, j, text)
    return text


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_punctuation(s):
    return ''.join(c for c in s if c not in punctuation)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import sample data for classification engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--path', default="./txt_dataset")
    parser.add_argument('--test', default=False)

    args = parser.parse_args()
    print(args)

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500)
    import_events(client, args.path, args.test)

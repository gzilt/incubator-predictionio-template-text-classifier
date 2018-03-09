import re, os,argparse, predictionio, uuid, string, csv
import xml.etree.cElementTree as ET
from html.parser import HTMLParser
import lxml
from lxml.html.clean import Cleaner


def import_events(client, path):

    cleaner = Cleaner()
    cleaner.javascript = True # This is True because we want to activate the javascript filter
    cleaner.style = True
    cleaner.scripts = True
    cleaner.comments = True
    cleaner.inline_style = True
    cleaner.kill_tags = ['a', 'sup', 'img']
    tempListCanonical = []
    tempListMetaTitle = []
    iabMap = {}
    runReport = {}
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
        # dataset specifics automobile
        r'(?s)(Related Articles.*?)(?:(?:\r*\n){}|~end~)': '',
        r'(?s)(Get Quote.*?)(?:(?:\r*\n){}|Get Quote)': '',
        # Remove full line starting with
        r'(?m)Related:.*\n?': '',
        # Remove full line starting with
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

    print('============================')
    print("Importing xml content...")
    print('============================')

    with open("lookup_iab.csv", 'r', encoding='UTF-8') as iab:
        reader = csv.reader(iab, delimiter=',')
        for item in reader:
            iabMap[item[1]] = item[0]

    files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.xml')]

    for file in files:
        count = 0
        noTitle = 0
        duplicateCount = 0
        contentTooShort = 0
        errorCount = 0
        categoryArr = file.split('_')

        categoryTemp = string.capwords(categoryArr[1])
        categoryIndex = re.sub('&', 'and',string.capwords(categoryArr[1]))
        categoryId = iabMap.get(categoryIndex)

        print("Importing data from file: {} under {}-{}".format(file, categoryId, categoryIndex))

        #tree = ET.parse('results_Dd6HLkMmdiR5ccNr2.xml')
        tree = ET.parse(path + '/' + file)
        root = tree.getroot()
        for child in root:
            metaTitle = child.find('meta_title')
            #print (elem.text)
            canonical = child.find('meta_canonical')
            content = child.find('content')
            if(content is not None):
                if(canonical is not None and canonical.text is not None):
                    if (canonical.text in tempListCanonical):
                        # print ()
                        # print ()
                        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                        # print ('found duplicate %s', canonical.text)
                        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                        # print ()
                        # print ()
                        duplicateCount += 1
                        continue
                    tempListCanonical.append(canonical.text)
                if (metaTitle is not None and metaTitle.text is not None):
                    if (metaTitle.text in tempListMetaTitle):
                        # print ()
                        # print ()
                        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                        # print ('found duplicate %s', metaTitle.text)
                        # print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                        # print ()
                        # print ()
                        duplicateCount += 1
                        continue
                    tempListMetaTitle.append(metaTitle.text)
                else:
                    noTitle += 1
                    continue


                title = replace_all(metaTitle.text.rstrip().replace('\n', ' ').replace('\r', ''), reps)

                #text = replace_all(content.text.rstrip().replace('\n', ' ').replace('\r', ''), reps)
                text = replace_all(strip_tags(cleaner.clean_html(content.text)).rstrip().replace('\n', ' ').replace('\r', ''), reps)


                if(title == text and len(text) < 256):
                    contentTooShort += 1
                    continue


                # print ()
                # #print ()
                # print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
                # print(title)
                print('--------------------------------------------------------------------------------------------------------------')
                print(text)
                # print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')

                count += 1

                # client.create_event(
                #     event="documents",
                #     entity_type="source",
                #     entity_id=str(count), # use the count num as user ID
                #     properties= {
                #         "text" : text,
                #         "category" : categoryIndex,
                #         "label" : int(categoryId)
                #     }
                # )
                # print("==>>>> Processing %s" % count)
                # item = elem.text
                # if(item):
                #     print (item)
            else:
                errorCount += 1

        print ()
        print ()
        print('****************************')
        print('* SUMMARY                  *')
        print('****************************')
        print("%s xml content are imported." % count)
        print("%s duplicates found." % duplicateCount)
        print("%s no title." % noTitle)
        print("%s content too short." % contentTooShort)
        print("%s error." % errorCount)
        print('****************************')
        if (categoryIndex in runReport):
            runReport[categoryIndex] += count
        else:
            runReport[categoryIndex] = count

    print (runReport)

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
        self.convert_charrefs= True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import sample data for classification engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--path', default="./dataset")

    args = parser.parse_args()
    print(args)

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500)
    import_events(client, args.path)

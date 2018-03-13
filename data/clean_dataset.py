import re, argparse, predictionio, string, csv, happybase, time, uuid
import xml.etree.cElementTree as ET
from itertools import islice
from html.parser import HTMLParser
from lxml.html.clean import Cleaner
from string import punctuation

batch_size = 10
host = "localhost"
namespace = "mcontent"
start_time = time.time()
table_name = "ai_training_data"


def import_events(client, file, output_type):
    temp_list_canonical = []
    temp_list_meta_title = []
    iab_map = {}
    run_report = {}
    word_counts = dict()
    data = []
    reps = {

        #technewsworld
        # r'TECH\sTREK': ' ',
        # r'CONFERENCE\sREPORT': ' ',
        # r'ANALYST\sCORNER': ' ',
        # r'LINUX\sPICKS\sAND\sPANS': ' ',
        # r'SPOTLIGHT\sON\sSECURITY': ' ',
        # r'GADGET\sDREAMS\sAND\sNIGHTMARES': ' ',
        # r'CROWDFUNDING\sSPOTLIGHT': ' ',
        # r'TECHNOLOGY\sLAW\sCORNER': ' ',
        # r'LINUX\sBLOG\sSAFARI': ' ',
        # r'GOVERNMENT\sIT\sREPORT': ' ',
        # r'WHICH\sAPPS\sDO\sI\sNEED?': ' ',
        # r'EXCLUSIVE\sINTERVIEW': ' ',
        # r'EXPERT\sADVICE': ' ',
        # r'PRODUCT\sPROFILE': ' ',
        # r'BOOK\sREVIEW': ' ',
        # r'ANDROID\sAPP\sREVIEW': ' ',
        # r'CRM\sBLOG\sSAFARI': ' ',
        # r'WEEKLY\sRECAP': ' ',
        # r'STARTUP\sTO\sWATCH': ' ',
        # r'WEEKEND\sFEATURE': ' ',
        # r'NEWS\sBRIEF': ' ',
        # r'TECH\sBLOG': ' ',
        # r'ANDROID\sAPP': ' ',
        # r'APP\sREVIEW': ' ',
        # r'OPINION': ' ',
        # r'PODCAST': ' ',
        # r'INSIGHTS': ' ',
        # r'ANALYSIS': ' ',
        # r'REVIEW': ' ',
        # r'PRODUCT': ' ',

        
        #autoblog
        #r'(?s)(Related\sVideo.*?)(?:(?:\r*\n){}|html5\sis\ssupported!)': ' ',
        #r'(?m).embed-container.*\n?': ' ',


        #jalopnik
        #r'Photo credit AP': ' ',
        #r'Photo credit': ' ',
        #r'Photo credit: Getty Images': ' ',
        #r'Photo: ': ' ',
        #r'Image: ': ' ',
        #r'Image credit': ' ',
        #r'Advertisement': ' ',
        #r'(?s)(Good\smorning!.*?)(?:(?:\r*\n){}|every\sweekday\smorning.)': ' ',
        #r'(?s)(This\sis\sThe\sMorning\sShift.*?)(?:(?:\r*\n){}|Read\smore\sRead)': ' ',
        #r'(?s)(This\sis\sThe\sMorning\sShift.*?)(?:(?:\r*\n){}|important\?)': ' ',

        #www.gq.com
        #r'(?m)View\son.*\n?': ' ',

        #lonelyplanet
        #r'(?m)/\s.*\n?': ' ',
        #r'(?m)/\sGetty.*\n?': ' ',


        #technology.inquirer.net
        #r'(?m)Recommended\sby.*\n?': ' ',

        #esquiremag
        #r'(?m)This\sstory\soriginally\sappeared\son\s.*\n?': ' ',


        #http://nolisoli.ph
        # r'(?m)TAGS:.*\n?': ' ',
        # r'(?m)TEXT\s.*\n?': ' ',
        # r'(?m)PHOTOGRAPHY\s.*\n?': ' ',


        #http://primer.com.ph
        #r'(?s)(HOME.*?)(?:(?:\r*\n){}|Fashion)': ' ',
        #r'(?m)Images\ssource.*\n?': ' ',
        #r'(?m)Written\sby.*\n?': ' ',
        #r'(?m)Google\sMap.*\n?': ' ',
        #'This\sis\sa\spress\srelease.'

        #ph.asiatatler.com


        #style and fashon HufMRPzrNisnJm9qW FemaleNetwork.com
        # r'(?m)More\sFrom.*\n?': ' ',
        # r'(?m)This\sstory\soriginally.*\n?': ' ',
        # r'(?m)\*\sMinor\sedits.*\n?': ' ',
        # r'(?m)IMAGE\sYOUTUBE.*\n?': ' ',
        # r'(?m)TRY:.*\n?': ' ',

        #popularmechanics
        #r'(?m)Getty\sImages.*\n?': ' ',
        #r'(?m)Related\sStory.*\n?': ' ',
        #r'(?s)(From:.*?)(?:(?:\r*\n){}|~)': ' ',
        #r'(?s)(Source:.*?)(?:(?:\r*\n){}|~)': ' ',

        # https: // www.homify.ph
        #r'(?s)(Open\sin\sa\snew\stab.*?)(?:(?:\r*\n){}|~)': ' ',
        #r'(?m)ID\sSave.*\n?': ' ',
        # r'(?m)ID.*\r?': ' ',
        # 'Need help with your home project?': '',
        # 'Get in touch!': '',
        # 'Request free consultation': '',
        # 'Garden Affairs Ltd': '',

        #gardeners.com
        #r'(?m)From\sGardener.*\n?': ' ',
        #r'(?s)(Related\sarticles.*?)(?:(?:\r*\n){}|~)': ' ',
        #r'(?s)(Related\sArticles.*?)(?:(?:\r*\n){}|~)': ' ',
        #r'(?m)Photo.*\n?': ' ',

        #http://lifestyle.inquirer.net
        # r'(?m)LOOK:.*\n?': ' ',
        # r'(?m)Recommended\sby.*\n?': ' ',
        # 'View comments': ' ',

        #https://www.rappler.com/life-and-style/career
        #https://www.rappler.com/life-and-style/best-eats
        # r'PH\sTravel.*\n?': ' ',
        # r'Travel.*\n?': ' ',
        # r'(?m)Movies.*\n?': ' ',
        # r'(?m)Oggs\sCruz.*\n?': ' ',
        # r'(?m)Photo\sby.*\n?': ' ',
        # r'(?m)JR\sIsaga.*\n?': ' ',
        # r'(?m)Delfin\sDioquino.*\n?': ' ',
        # r'(?m)Entertainment\sNews.*\n?': ' ',
        # r'Agence\sFrance-Presse': ' ',
        # '@afp': ' ',
        # r'Ryan\sSongalia': ' ',
        # r'Jill\sTan\sRadovan': ' ',
        # r'Iñigo\sde\sPaula': ' ',
        # r'Alexa\sVillano': ' ',
        # r'Oscar\sH.\sPurugganan': ' ',
        # r'Jacklynne\sLambino': ' ',
        # r'Mavic\sConde': ' ',
        # r'Rhea\sClaire\sMadarang': ' ',
        # r'Rhea\sClaire\sE\sMadarang': ' ',
        # r'Glen\sSantillan': ' ',
        # r'Joshua\sBerida': ' ',
        # r'Earnest\sMangulabnan\sZabala': ' ',
        # r'Tommy\sWalker': ' ',
        # r'Jona\sBranzuela\sBering': ' ',
        # r'Ceej\sTantengco': ' ',
        # r'Aleah\sTaboclaon': ' ',
        # '@alexavillano': ' ',
        # '@ryansongalia': ' ',
        # r'Anne\sMari\sRonquillo': ' ',
        # r'(?m)Gelo\sGonzales.*\n?': ' ',
        # r'Photo\scourtesy\sof.*\n?': ' ',
        # r'(?m)Tech\sFeatures.*\n?': ' ',
        # r'(?m)Published.*\n?': ' ',
        # r'(?m)Updated.*\n?': ' ',
        # r'(?m)Review:.*\n?': ' ',
        # 'Rappler.com': ' ',


        # autodeal.com.ph
        #r'(?s)(Related\sArticles.*?)(?:(?:\r*\n){}|~)': ' ',
        #r'(?s)(Get\sQuote.*?)(?:(?:\r*\n){}|Get\sQuote)': ' ',

        # topgear.com.ph
        #r'(?s)(Other\sRelated\sStories.*?)(?:(?:\r*\n){}|~)': ' ',

        #https://www.cosmo.ph
        #r'(?s)(More from Cosmo.ph.*?)(?:(?:\r*\n){}|~)': ' ',
        #'Follow Ira on Instagram.': ' ',

        #www.candymag.com
        #'( @ gettyimages)': ' ',

        #www.bitesized.ph
        # r'(?s)(\ |.* ?)(?:(?:\r *\n){} | shares)': ' ',
        # r'(?s)(\ |.* ?)(?:(?:\r *\n){} | likes)': ' ',
        # r'(?s)(Share this:.*?)(?:(?:\r*\n){}|~)': ' ',
        # r'(?s)(DIFFICULTY:.*?)(?:(?:\r*\n){}|Easy)': ' DIFFICULTY: Easy ',
        # r'(?s)(DIFFICULTY:.*?)(?:(?:\r*\n){}|Moderate)': ' DIFFICULTY: Moderate ',
        # r'(?s)(DIFFICULTY:.*?)(?:(?:\r*\n){}|Easy\/Moderate)': 'DIFFICULTY: Easy/Moderate ',
        # 'Ingredients:': ' Ingredients: ',
        # 'INGREDIENTS:': ' INGREDIENTS: ',

        #www.townandcountrymag.com
        #r'(?s)(This\sstory\soriginally\sappeared\son\sTownandcountrymag.com.*?)(?:(?:\r*\n){}|~)': ' ',
        #r'(?m)ShareIMAGE.*\n?': ' ',
        #'More from Townandcountry.ph': ' ',


        # https://www.everydayhealth.com
        # r'(?s)(ADVERTISING.*?)(?:(?:\r*\n){}|the close button)': '',
        # r'(?s)(SharePlay.*?)(?:(?:\r*\n){}|the close button.)': '',
        # 'Close Modal DialogThis is a modal window. This modal can be closed by pressing the Escape key or activating the close button.': '',

        # http://www.esquire.com
        #r'(?m)Illustration\sby.*\n?': ' ',
        #r'(?m)Courtesy\sof.*\n?': ' ',
        # 'PLUS:.+>>': ' ',
        # r'(?m)Plus. *\n?': '',
        # 'If you work in the food industry and are interested in writing for The Spill, please send your ideas to spill@esquire.com.': '',

        # www.breakfastmag.com
        # r'(?s)(close full screen.*?)(?:(?:\r*\n\f\t){}|Your email)': '',
        # '0000': '',
        # 'Share': '',

        # https://www.yummy.ph
        #r'(?m)More from.*\n?': ' ',
        #'WATCH:': ' ',

        # https://www.clickthecity.com
        # r'(?s)(You May LikeDermalMedix.*?)(?:(?:\r*\n){}|Links Promoted Links)': '',
        # 'Promoted Links': '',

        r'<[^>]+>': ' ',

        # common pattern
        r'(?m)ShareIMAGE\s.*\n?': ' ',
        r'(?m)Advertisement.*\n?': ' ',
        r'(?m)ADVERTISEMENT.*\n?': ' ',
        r'(?m)Getty Images.*\n?': ' ',
        r'(?m)Related Story.*\n?': ' ',
        r'(?m)Photography by.*\n?': ' ',
        r'(?m)Photo\sby. *\n?': ' ',
        r'(?m)via GIPHY.*\n?': ' ',
        r'(?m)Text by.*\n?': ' ',
        r'(?m)Art by.*\n?': ' ',
        r'(?m)IMAGE.*\n?': ' ',
        r'(?m)Image.*\n?': ' ',
        r'(?m)PHOTOS.*\n?': ' ',
        r'(?m)More from.*\n?': ' ',
        r'(?m)Related:.*\n?': ' ',
        r'\[image.+]': ' ',
        '&nbsp;': ' ',
        '///': ' ',
        '’': '\'',
        '`': '\'',
        '"': ' ',
        '–': '-',
        r'\u2014': '-',
        r'\u2026': ' ',
        r'\u2022': ' ',
        r'\u0022': ' ',
        r'\s+': ' ',
        '~': ' '
    }

    cleaner = Cleaner()
    cleaner.javascript = True  # This is True because we want to activate the javascript filter
    cleaner.style = True
    cleaner.scripts = True
    cleaner.comments = True
    cleaner.inline_style = True
    cleaner.kill_tags = ['a', 'sup', 'img']

    print('============================')
    print("Importing xml content...")
    print('============================')

    with open("lookup_iab.csv", 'r', encoding='UTF-8') as iab:
        reader = csv.reader(iab, delimiter=',')
        for item in reader:
            iab_map[item[1]] = item[0]

    count = 0
    no_title = 0
    duplicate_count = 0
    content_too_short = 0
    error_count = 0
    category_arr = file.split('_')

    category_index = re.sub('&', 'and', string.capwords(category_arr[1]))
    category_id = iab_map.get(category_index)

    print("Importing data from file: {} under {}-{}".format(file, category_id, category_index))

    # After everything has been defined, run the script.
    #conn, batch = connect_to_hbase()
    #conn, table = connect_to_hbase()
    print("Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size))

    new_file = open('./txt_dataset/' + re.sub('.xml', '.txt', file), 'w', encoding='UTF-8')

    tree = ET.parse('./dataset/' + file)

    root = tree.getroot()
    for child in root:
        has_error = child.find('errorInfo')

        if has_error is not None:
            error_count += 1
            continue

        #meta_title = child.find('meta_title')
        canonical = child.find('meta_canonical')
        content = child.find('content')
        #loadTime = child.find('loadTime')

        url = ''
        if content is not None and content.text is not None:
            if canonical is not None and canonical.text is not None:
                if canonical.text in temp_list_canonical:
                    duplicate_count += 1
                    continue
                temp_list_canonical.append(canonical.text)
                url = canonical.text
            # if meta_title is not None and meta_title.text is not None:
            #     if meta_title.text in temp_list_meta_title:
            #         duplicate_count += 1
            #         continue
            #     temp_list_meta_title.append(meta_title.text)
            # else:
            #     no_title += 1
            #     continue
            #
            # title = replace_all(meta_title.text.rstrip().replace('\n', ' ').replace('\r', ''), reps)
            temp_text = content.text

            #print('ERROR')
            #print(loadTime.text)

            temp = replace_all(content.text, reps).rstrip().replace('\n', ' ').replace('\r', ' ')

            if temp is None or temp == '':
                error_count += 1
                continue

            print('--'+temp+'&&&')

            text = strip_tags(cleaner.clean_html(temp))

            # if title == text or len(text) < 500:
            #     content_too_short += 1
            #     continue
            if len(text) < 500:
                content_too_short += 1
                continue

            # print ()
            # print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
            # print(title)
            #print('--------------------------------------------------------------------------------------------------------------')
            #print(text)
            # print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')

            # words = strip_punctuation(text).split()
            # for word in words:
            #     if word in word_counts:
            #         word_counts[word] += 1
            #     else:
            #         word_counts[word] = 1

            #insert_row(batch, {'category': category_index, 'category_id': category_id, 'url': url, 'title': title, 'content': text})
            #insert_row(table, {'category': category_index, 'category_id': category_id, 'url': url, 'title': title,
            #                   'content': text})

            #data.append({'category': category_index, 'category_id': category_id, 'url': url, 'title': title, 'content': text})
            count += 1

            new_file.write(text + "\n")
        else:
            error_count += 1

    print()
    print('****************************')
    print('* SUMMARY                  *')
    print('****************************')
    print("%s xml content are imported." % count)
    print("%s duplicates found." % duplicate_count)
    print("%s no title." % no_title)
    print("%s content too short." % content_too_short)
    print("%s error." % error_count)
    print('****************************')
    if category_index in run_report:
        run_report[category_index] += count
    else:
        run_report[category_index] = count

    # n_items = take(10, data)
    # for items in n_items:
    #     print(items['category'])
    #batch.send()
    print(run_report)
    # for word, count in word_counts.items():
    #     if int(count) > 50:
    #         print("{} = {}".format(word, count))

    new_file.close()


def connect_to_hbase():
    """ Connect to HBase server.
    This will use the host, namespace, table name, and batch size as defined in
    the global variables above.
    """
    conn = happybase.Connection(host=host, table_prefix=namespace, table_prefix_separator=":", port=61134)
    #conn.open()
    table = conn.table(table_name)
    #batch = table.batch(batch_size=batch_size)
    #return conn, batch
    return conn, table


#def insert_row(batch, row):
def insert_row(table, row):
    """ Insert a row into HBase.
    Write the row to the batch. When the batch size is reached, rows will be
    sent to the database.
    Rows have the following schema: [category, category_id, url, title, content]
    """
    table.put(str(uuid.uuid4()), {b'data:category': row['category'], b'data:category_id': row['category_id'], b'data:url': row['url'], b'data:title': row['title'], b'data:content': row['content']})


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


def take(n, iterable):
    """Return first n items of the iterable as a list"""
    return list(islice(iterable, n))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import sample data for classification engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")
    parser.add_argument('--file', default="AI-TRAINING_TRAVEL_MuCNpRK8E3EdJ8h4u.xml")
    parser.add_argument('--output_type', default="json")

    args = parser.parse_args()
    print(args)

    client = predictionio.EventClient(
        access_key=args.access_key,
        url=args.url,
        threads=5,
        qsize=500)
    import_events(client, args.file, args.output_type)
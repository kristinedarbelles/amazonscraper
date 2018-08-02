import requests
# Based on a Google search, the most popular Python package used to scrape web pages.
from bs4 import BeautifulSoup
from datetime import datetime
import time
import sqlite3
import sys
import urllib.request

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)
    return None

def insert_review(conn, ReviewID,Date,Starrating,Title,Body,Username,UserID,Verified,Helpful,Comments):
    sql = ''' INSERT INTO ReviewsMakey(ReviewID,Date,Starrating,Title,Body,Username,UserID,Verified,Helpful,Comments)
              VALUES(?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (ReviewID,Date,Starrating,Title,Body,Username,UserID,Verified,Helpful,Comments))
    conn.commit()
    return cur.lastrowid

# Code below is scraping reviews for Tracer360
urlstring = 'https://www.amazon.com/Makey-Invention-Kit-Everyone/product-reviews/B008SFLEPE/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36'}

req = urllib.request.Request(urlstring, headers=headers)
page = urllib.request.urlopen(req)

#print(page.read())

pagenumber = 1
totalreviews = 0
myconnection = create_connection("database/data.db")
if myconnection is None:
    print("could not connect to database. aborting.")
    sys.exit(1)

# Using a while loop to scrape all pages with reviews.
while True:
    print('Processing page: {}'.format(pagenumber))
    soup = BeautifulSoup(page.read(),"html.parser")

    review_list = soup.find("div", {"id": 'cm_cr-review_list'})
    # Encountered an error when 'cm_cr-review_list' wasn't found, so I consulted a Python expert who helped me create a condition to try again.
    if review_list is None:
        time.sleep(1)
        req = urllib.request.Request(urlstring, headers=headers)
        page = urllib.request.urlopen(req)

        print('Processing page: {}'.format(pagenumber))
        soup = BeautifulSoup(page.read(), "html.parser")

        review_list = soup.find("div", {"id": 'cm_cr-review_list'})

    # Here I am extracting the code that contains the list of reviews.
    reviews = review_list.findChildren(attrs={"data-hook": "review"}, recursive=False)

    # This is the code to stop the loop when all pages have been scraped.
    if len(reviews) == 0:
        break
    for review in reviews:
        # Initializing variables to none
        output_id = None
        output_datespan = None
        output_starrating = None
        output_title = None
        output_body = None
        output_authorname = None
        output_authorid = None
        output_avp = None
        output_helpful = None
        output_comment = None

        totalreviews = totalreviews + 1

        # Extracting the ID of each review.
        output_id = review.get("id")
        print("id is {}".format(output_id))

        # Extracting the date the review was published, and making sure the object is a datetime object. This will help with database queries during data analysis.
        datespans = review.findChildren(attrs={"data-hook": "review-date"}, recursive=True)
        for datespan in datespans:
            output_datespan = datespan.text.replace("on ","")
            output_datespan = datetime.strptime(output_datespan,"%B %d, %Y")
            print("date is {}".format(output_datespan))

        # Extracting the star rating, and making sure the object is an integer.
        starratings = review.findChildren(attrs={"data-hook": "review-star-rating"}, recursive=True)
        for starrating in starratings:
            for classname in starrating.get("class"):
                if "a-star-" in classname:
                    output_starrating = int(classname.replace("a-star-",""))
                    print("star rating is {}".format(output_starrating))

        # Extracting title of review.
        titles = review.findChildren(attrs={"data-hook": "review-title"}, recursive=True)
        for title in titles:
            output_title = title.text
            print("title is {}".format(output_title))

        # Extracting body content of review.
        bodies = review.findChildren(attrs={"data-hook": "review-body"}, recursive=True)
        for body in bodies:

            # Amazon includes HTML code in the body. I consulted a Python expert to write code that would remove the HTML. This will allow me to more easily run a bag of words analysis.
            bodylist = [item for item in body.contents if isinstance(item,str)]
            output_body = " ".join(bodylist)
            print("body is {}".format(output_body))

        # Extracting author of review. I extracted both the username and ID.
        authors = review.findChildren("a", attrs={"data-hook": "review-author"}, recursive=True)
        for author in authors:
            output_authorname = author.text
            output_authorid = author.get("href").split(".")[-1]
            print("author is {}".format(output_authorname))
            print("account id is {}".format(output_authorid))

        # This is testing whether or not the review was posted by a "verified purchaser". Object is a boolean object. This will help with database queries.
        avps = review.findChildren(attrs={"data-hook": "avp-badge"}, recursive=True)
        output_avp = True if len(avps) >0 else False
        print("verified purchaser: {}".format(output_avp))

        # Extracting the number of people found each review helpful, and making sure the object is an integer.
        helpfulstatements = review.findChildren(attrs={"data-hook": "helpful-vote-statement"}, recursive=True)
        for helpfulstatement in helpfulstatements:
            output_helpful = helpfulstatement.text.split(" ")[0]

            # When only one person finds a review helpful, Amazon displays the content as the word "One". I consulted a Python expert to help add code to handle the scenario - converting the string "One" to the integer 1.
            output_helpful = 1 if output_helpful == "One" else int(output_helpful)
            print("Number of helpful votes is {}".format(output_helpful))

        # Extracting the number of comments on each review, and making sure the object is an integer.
        comments = review.findChildren("span", attrs={"class": "review-comment-total"}, recursive=True)
        for comment in comments:
            output_comment = int(comment.text)
            print("Number of comments is {}".format(output_comment))

        print("---------------------------")

        insert_review(myconnection, output_id,output_datespan,output_starrating,output_title,output_body,output_authorname,
                  output_authorid,output_avp,output_helpful,output_comment)

    # We pause the scraping for one second. Other coders who have scraped Amazon have suggested following this best practice so that Amazon doesn't block your code.
    time.sleep(1)

    # Here I am getting the next page. Once the next page is obtained, it goes back to the top of the loop where it collects all the above information for reviews on that page.
    pagenumber = pagenumber + 1
    urlstring = 'https://www.amazon.com/Makey-Invention-Kit-Everyone/product-reviews/B008SFLEPE/ref=cm_cr_arp_d_paging_btm_{}?ie=UTF8&reviewerType=all_reviews&pageNumber={}'.format(pagenumber, pagenumber)
    req = urllib.request.Request(urlstring, headers=headers)
    page = urllib.request.urlopen(req)

myconnection.close()
print("Total reviews = {}".format(totalreviews))

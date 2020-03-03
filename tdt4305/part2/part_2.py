import base64
import sys

from pyspark import SparkConf, SparkContext
from operator import add
from .. import utils


import time
def timeit(method):
    def timed(*args, **kw):
        ts = time.perf_counter()
        result = method(*args, **kw)
        te = time.perf_counter()
        diff = (te - ts)*1000
        print(f"{method.__name__} executed in {diff:.8f} ms")
        return result
    return timed





INPUT_DIR = "./data"
OUTPUT_DIR = "./results"
RESOURCES_DIR = "./resources"

SAMPLES_INPUT_DIR = "./samples/data"
SAMPLES_OUTPUT_DIR = "./samples/results"

RT_FILENAME = "yelp_top_reviewers_with_reviews.csv"

def initialize():
    conf = SparkConf()
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")
    print('\n'*100)
    return spark_context

def load_reviews(spark_context, input_dir):
    # Load RDD
    path = utils.gen_path(input_dir, RT_FILENAME)
    print(F"PATH = {path}")
    rdd_strings = spark_context.textFile(path)
    rt_rdd_raw = rdd_strings.map(lambda line: line.split("\t"))

    # Convert RDD into a format suitable for further analysis.
    rt_header = rt_rdd_raw.first() 
    rt_rdd = rt_rdd_raw.filter(lambda row: row != rt_header)

    # Decode review text strings
    def review_text_decoder(row):
        review_text_b64 = row[3]
        review_text_bytes = base64.b64decode(review_text_b64)
        review_text = review_text_bytes.decode('utf-8', errors='replace')
        row[3] = review_text
        return row
    rt_rdd = rt_rdd.map(review_text_decoder)

    # Change storage policy
    rt_rdd.persist()
    return rt_rdd

def tokenize(text):
    return text.split() # Split text on any whitespace

def clean_text(tokens):
    return [''.join(c.lower() for c in token if c.isalpha()) for token in tokens]

def remove_stopwords(tokens, stopwords):
    return list(token for token in tokens if token not in stopwords)

def evaluate_score(tokens, afinn):
    return sum(afinn.get(token, 0) for token in tokens)

@timeit
def sentiment(text, stopwords, afinn):
    tokens = tokenize(text)
    tokens = clean_text(tokens)
    tokens = remove_stopwords(tokens, stopwords)
    return evaluate_score(tokens, afinn)

def run(spark_context, input_dir, k=10):
    # Load stopwords
    STOPWORDS_PATH = f"{RESOURCES_DIR}\stopwords.txt"
    with open(STOPWORDS_PATH) as f:
        stopwords = {line.strip() for line in f}
        
    # Load afinn
    AFINN_PATH = f"{RESOURCES_DIR}\AFINN-111.txt"
    with open(AFINN_PATH) as f:
        afinn = {k: int(v) for k, v in (line.split('\t') for line in f)}

    # # Load RDDs (task 1a) and dataframes (task 5a)    
    rt_rdd = load_reviews(spark_context, input_dir)
    review_scores_rdd = rt_rdd.map(lambda row: (row[2], sentiment(row[3], stopwords, afinn)))

    # Group by business ID
    top_k_businesses = review_scores_rdd.reduceByKey(add).top(k, key=lambda tup: tup[1])

    # Print top 10 businesses
    print(f" --- TOP {k} BUSINESSES --- ")
    for business_id, score in top_k_businesses:
        print(f"{business_id}\t{score}")

def export_txt(spark_context, input_dir, output_dir):
    '''Redirects console output to file.'''
    path = f"{output_dir}/output.txt"
    print(f"Writing console output to '{path}' ...")
    with open(path, 'w') as sys.stdout:
        run(spark_context, input_dir)


def main(action, sample):
    sample = True
    input_dir = SAMPLES_INPUT_DIR if sample else INPUT_DIR
    output_dir = SAMPLES_OUTPUT_DIR if sample else OUTPUT_DIR
    spark_context = initialize()

    if action == 'run':
        run(spark_context, input_dir)
    elif action == 'export-txt':
        export_txt(spark_context, input_dir, output_dir)


# if __name__ == "__main__":
#     testrun(reviews)

# Test data
# review_1 = "Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Aenean commodo ligula eget dolor. Aenean massa. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Donec quam felis,"
# review_2 = "ultricies nec, pellentesque eu, pretium quis, sem. Nulla consequat massa quis enim. Donec pede justo, fringilla vel, aliquet nec, vulputate eget, arcu. In enim justo, rhoncus ut, imperdiet a, venenatis vitae, justo. Nullam dictum felis eu pede mollis pretium. Integer tincidunt. Cras dapibus. Vivamus elementum semper nisi. Aenean vulputate eleifend tellus. Aenean leo ligula, porttitor eu, consequat vitae, eleifend ac, enim. Aliquam lorem ante, dapibus in, viverra quis, feugiat a, tellus."
# review_3 = "Phasellus viverra nulla ut metus varius laoreet. Quisque rutrum. Aenean imperdiet. Etiam ultricies nisi vel augue. Curabitur ullamcorper ultricies nisi. Nam eget dui. Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante. Etiam sit amet orci eget eros faucibus tincidunt. Duis leo. Sed fringilla mauris sit amet nibh. Donec sodales sagittis magna. Sed consequat, leo eget bibendum sodales, augue velit cursus nunc,"
# review_4 = """The generator doles out its contents one at a time. If you try to look at the generator itself, it doesn't dole anything out and you just see it as "generator object". To get at its contents, you need to iterate over them. You can do this with a for loop, with the next function, or with any of various other functions/methods that iterate over things (str.join among them).
# When you say that result "is a list of string" you are getting close to the idea. A generator (or iterable) is sort of like a "potential list". Instead of actually being a list of all its contents all at once, it lets you peel off each item one at a time.
# None of the objects is a "memory address". The string representation of a generator object (like that of many other objects) includes a memory address, so if you print it (as above) or write it to a file, you'll see that address. But that doesn't mean that object "is" that memory address, and the address itself isn't really usable as such. It's just a handy identifying tag so that if you have multiple objects you can tell them apart."""
# review_5 = """I love beautiful movies. If a film is eye-candy with carefully designed decorations, masterful camerawork, lighting, and architectural frames, I can forgive anything else in the movie. The lack or even absence of logic, weak dialogue and characters, cliche storylines–I can live with all that if I like what I see.
# And I am not talking about popcorn summer blockbusters here, Marvel or DC screen versions of comics, Transformers, and so on. Perhaps, one of the best examples of what matters in a movie to me is “Ghost in the Shell”–an absolute failure in terms of succeeding its anime source in everything but visuals. I am a fan of the original cartoon, and although I did not expect much from a Hollywood adaptation, I still was not prepared for what I saw: a bleak, dull movie full of cliches and suffering from huge plot holes. The two aspects of the movie I liked, however, were the scenes copied from the anime, and the imagery. I must admit, I absolutely loved the visuals in “Ghost in the Shell.” An insanely stylish, bright, cyberpunk-ish mixture of traditional, modern, and futuristic elements in outfits. The character design amazed me so much that I rewatched “Ghost in the Shell” twice just because of how it looked."""
# reviews = [review_1, review_2, review_3, review_4, review_5]

# for review in reviews:
#     print(sentiment(review, stopwords, afinn))

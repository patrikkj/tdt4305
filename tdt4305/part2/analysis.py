from operator import add


def tokenize(text):
    return text.split()

def clean_text(tokens):
    return [''.join(c.lower() for c in token if c.isalpha()) for token in tokens]

def remove_stopwords(tokens, stopwords):
    return list(token for token in tokens if token not in stopwords)

def evaluate_score(tokens, afinn):
    return sum(afinn.get(token, 0) for token in tokens)

def sentiment(text, stopwords, afinn):
    tokens = tokenize(text)
    tokens = clean_text(tokens)
    tokens = remove_stopwords(tokens, stopwords)
    return evaluate_score(tokens, afinn)

def analyze(rt_rdd, afinn, stopwords, k):
    review_scores_rdd = rt_rdd.map(lambda row: (row[2], sentiment(row[3], stopwords, afinn)))
    return review_scores_rdd.reduceByKey(add).top(k, key=lambda tup: tup[1])
    
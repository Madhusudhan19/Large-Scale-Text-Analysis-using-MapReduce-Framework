from mrjob.job import MRJob
import re

# Regular expression to split lines into words
WORD_RE = re.compile(r"[\w']+")

class MRWordCount(MRJob):

    # 1. The Mapper function: Breaks text lines into (word, 1) pairs
    def mapper(self, _, line):
        for word in WORD_RE.findall(line.lower()):
            yield (word, 1)

    # 2. The Reducer function: Sums up all the counts for a given word
    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordCount.run()

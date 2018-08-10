from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularHero(MRJob):

    def mapper(self, _, line):
        line_arr = line.split(" ")
        hero = line_arr[0]
        friends = [int(x) for x in line_arr[1:] if len(x) > 0]
        
        for friend in friends:
            yield (int(hero), 1)

    def combiner_count_friends(self, hero, counts):
        # sum the hero friend's
        yield (hero, sum(counts))

    def reducer_count_friends(self, hero, counts):
        # send all (counts, hero) pairs to the same reducer.
        # counts is so we can easily use Python's max() function.
        yield None, (sum(counts), hero)

    # discard the key; it is just None
    def reducer_find_max_friends_hero(self, _, hero_count_pairs):
        # each item of hero_count_pairs is (count, hero),
        # so yielding one results in key=counts, value=hero
        yield max(hero_count_pairs)

    def steps(self):
        return [MRStep(mapper=self.mapper,
                    combiner=self.combiner_count_friends,
                    reducer=self.reducer_count_friends),
                MRStep(reducer=self.reducer_find_max_friends_hero)]

if __name__ == '__main__':
    MRMostPopularHero.run()
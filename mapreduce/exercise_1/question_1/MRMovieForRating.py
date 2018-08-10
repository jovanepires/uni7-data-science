from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMovieForRating(MRJob):

    def mapper(self, _, line):
        (_, movie_id, rating, _) = line.split("\t")
        yield (int(movie_id), int(rating))

    def reducer_avg_rating(self, movie_id, ratings):
        ratings_list = list(ratings)
        average = round(sum(ratings_list)/len(ratings_list), 2)
        yield None, (average, movie_id)

    def reducer_sort(self, _, movies):
        for movie in sorted(list(movies)):
            yield movie
    
    def steps(self):
        return [MRStep(
                    mapper=self.mapper,
                    reducer=self.reducer_avg_rating),
                MRStep(
                    reducer=self.reducer_sort)]

if __name__ == '__main__':
    MRMovieForRating.run()
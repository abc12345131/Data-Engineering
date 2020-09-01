CREATE OR REPLACE MODEL
  movies.movie_recommender
OPTIONS
  (model_type='matrix_factorization',
    user_col='userId',
    item_col='movieId',
    rating_col='rating',
    l2_reg=0.2,
    num_factors=16) AS
SELECT
  userId,
  movieId,
  rating
FROM
  movies.movielens_ratings
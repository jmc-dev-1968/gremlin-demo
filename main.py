
import sys
import boto3
import config
import neptune_utils as nu
import config
import pandas as pd
import os
import io
import csv
import uuid
from tabulate import tabulate
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P, Order


def summarize_graph(msg=""):

    if msg:
        print(msg)

    # make connection to websocket
    neptune = nu.NeptuneWSConnection()
    g = neptune.get_traversal()

    vcnt1 = g.V().hasLabel('movie').count().next()
    vcnt2 = g.V().hasLabel('actor').count().next()
    vcnt3 = g.V().hasLabel('director').count().next()
    ecnt1 = g.E().hasLabel('acted_in').count().next()
    ecnt2 = g.E().hasLabel('directed_by').count().next()
    data = {
        'object': ['vertex (movie)', 'vertex (actor)', 'vertex (director)', 'edge (actor-movie)', 'edge (movie-director)'],
        'count': [vcnt1, vcnt2, vcnt3, ecnt1, ecnt2]
    }
    df = pd.DataFrame(data)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    neptune.close()


def clean_graph():

    summarize_graph("\nbefore cleaning")

    # make connection to websocket
    neptune = nu.NeptuneWSConnection()
    g = neptune.get_traversal()

    # (1) delete all edges
    g.E().hasLabel('acted_in').drop().iterate()
    g.E().hasLabel('directed_by').drop().iterate()

    # (2) delete all movie vertices
    g.V().hasLabel('movie').drop().iterate()
    g.V().hasLabel('actor').drop().iterate()
    g.V().hasLabel('director').drop().iterate()

    neptune.close()

    summarize_graph("\nafter cleaning")


def analyze_graph():

    # make connection to websocket
    neptune = nu.NeptuneWSConnection()
    g = neptune.get_traversal()

    msg =  "(1) sample vertices (movies)"
    print("\n{}\n".format(msg))
    movies = g.V().hasLabel('movie').limit(10).project('name', 'year', 'director').by('name').by('year').by('director').toList()
    df = pd.DataFrame(movies)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    msg =  "(2) sample vertices (actors)"
    print("\n{}\n".format(msg))
    actors = g.V().hasLabel('actor').limit(10).project('name').by('name').toList()
    df = pd.DataFrame(actors)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    msg =  "(3) sample vertices (directors)"
    print("\n{}\n".format(msg))
    actors = g.V().hasLabel('director').limit(10).project('name').by('name').toList()
    df = pd.DataFrame(actors)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    msg =  "(4) top 10 actors who have starred with the most other actors"
    print("\n{}\n".format(msg))
    q = (
        g.V().hasLabel('actor').as_('a')
        .project('actor', 'fellow_actor_count')
        .by('name')
        .by(
            __.out('acted_in')
            .in_('acted_in')
            .where(P.neq('a'))
            .dedup()
            .count()
        )
        .order().by(__.select('fellow_actor_count'), Order.desc)
        .limit(10)
    )
    results = q.toList()
    df = pd.DataFrame(results)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    msg =  "(5) top 5 directors with the most movies"
    print("\n{}\n".format(msg))
    q = (
        g.V().hasLabel('director')
        .project('director', 'movie_count')
        .by('name')
        .by(__.in_('directed_by').count())
        .order().by(__.select('movie_count'), Order.desc)
        .limit(5)
    )
    results = q.toList()
    df = pd.DataFrame(results)[['director', 'movie_count']]
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    msg =  "(6) top 10 actors who have starred with both de niro and pacino"
    print("\n{}\n".format(msg))
    q = (
        g.V().hasLabel('actor').has('name', 'Robert De Niro')
        .out('acted_in').in_('acted_in')  # get all actors robert de niro worked with
        .where(
            __.out('acted_in').in_('acted_in')  # check if they also worked with
            .hasLabel('actor').has('name', 'Al Pacino')  # al pacino
        )
        .where(__.values('name').is_(P.neq('Robert De Niro')))  # exclude robert de niro himself
        .where(__.values('name').is_(P.neq('Al Pacino')))  # exclude al pacino himself
        .dedup()
        .project('shared_actor')
        .by('name')
        .limit(10)
    )

    results = q.toList()
    df = pd.DataFrame(results)
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

    msg =  "(7) top 10 actors who have starred with both de niro and pacino, and their movies"
    print("\n{}\n".format(msg))

    # find actors who worked with robert de niro
    deniro_actors = (
        g.V().hasLabel('actor').has('name', 'Robert De Niro')
        .out('acted_in').in_('acted_in')
        .where(__.values('name').is_(P.neq('Robert De Niro')))
        .project('actor', 'movie')
        .by('name')
        .by(__.out('acted_in').where(__.in_('acted_in').has('name', 'Robert De Niro')).values('name'))
        .toList()
    )

    # find actors who worked with al pacino
    pacino_actors = (
        g.V().hasLabel('actor').has('name', 'Al Pacino')
        .out('acted_in').in_('acted_in')
        .where(__.values('name').is_(P.neq('Al Pacino')))
        .project('actor', 'movie')
        .by('name')
        .by(__.out('acted_in').where(__.in_('acted_in').has('name', 'Al Pacino')).values('name'))
        .toList()
    )

    # process in pandas
    df_deniro = pd.DataFrame(deniro_actors)
    df_pacino = pd.DataFrame(pacino_actors)

    # find shared actors
    shared_actors = set(df_deniro['actor']) & set(df_pacino['actor'])

    # create separate columns for de niro and pacino movies
    all_movies = []
    for actor in shared_actors:
        deniro_movies = list(set(df_deniro[df_deniro['actor'] == actor]['movie'].tolist()))
        pacino_movies = list(set(df_pacino[df_pacino['actor'] == actor]['movie'].tolist()))

        all_movies.append({
            'shared_actor': actor,
            'movies (de niro)': ', '.join(sorted(deniro_movies)),
            'movies (pacino)': ', '.join(sorted(pacino_movies)),
            'total_movies': len(deniro_movies) + len(pacino_movies)
        })

    # create dataframe and sort by total movie count descending
    df_result = pd.DataFrame(all_movies)
    df_result = df_result.sort_values('total_movies', ascending=False)
    df_result = df_result[['shared_actor', 'movies (de niro)', 'movies (pacino)']]

    print(tabulate(df_result, headers='keys', tablefmt='psql', showindex=False))

    neptune.close()


""" df to local file system """
def df_to_local_fs(df_input, output_file):

    data_dir = config.LOCAL_DATA_DIR
    csv_buffer = io.StringIO()
    df_input.to_csv(csv_buffer, index=False, header=True, quoting=csv.QUOTE_ALL, quotechar='"')
    csv_file_path = "{}{}{}".format(data_dir, os.sep, output_file)
    csv_string = csv_buffer.getvalue()
    try:
        with open(csv_file_path, 'w', encoding="utf8", newline='') as f:
            f.write(csv_string)
        print("successfully saved '{}'".format(output_file))
    except Exception as e:
        print("error saving file: {}".format(e))


""" df to local s3 bucket """
def df_to_s3(df_input, output_file, prefix=""):

    data_dir = config.LOCAL_DATA_DIR
    csv_buffer = io.StringIO()
    df_input.to_csv(csv_buffer, index=False, header=True, quoting=csv.QUOTE_ALL, quotechar='"')
    csv_string = csv_buffer.getvalue()
    bytes_buffer = io.BytesIO(csv_string.encode('utf-8'))

    s3_client = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION
    )

    bucket_name = config.S3_BUCKET
    bucket_prefix = config.S3_PREFIX
    if prefix:
        object_key = "{}/{}/{}".format(bucket_prefix, prefix, output_file)
    else:
        object_key = "{}/{}".format(bucket_prefix, output_file)
    try:
        s3_client.upload_fileobj(bytes_buffer, bucket_name, object_key)
        print("successfully uploaded {}'".format(object_key))
    except Exception as e:
        print("error uploading object {} : {}".format(object_key, e))


def s3_to_df(csv_file, prefix=""):

    s3_client = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION
    )
    bucket_name = config.S3_BUCKET
    bucket_prefix = config.S3_PREFIX
    if prefix:
        object_key = "{}/{}/{}".format(bucket_prefix, prefix, csv_file)
    else:
        object_key = "{}/{}".format(bucket_prefix, csv_file)
    df = None
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        print("successfully downloaded '{}'".format(object_key))
    except Exception as e:
        print("error downloading object {} : {}".format(object_key, e))
    return df


def process_data(save_to_s3=True, save_to_local=True):

    """

    sample data found here (https://www.kaggle.com/datasets/harshitshankhdhar/imdb-dataset-of-top-1000-movies-and-tv-shows)

    this is a csv file of 1000 movies from imdb.com, with the first 4 actors in each movie (pivoted to columns, so data looks
    like so (only relevant columns shown)

      Series_Title, Released_Year, Director, Star1, Star2, Star3, Star4

    this relational data (in a semi-normalized format) must be parsed and separated iinto a set of "graphdb csv"
    datasets, one set for vertices and one set for edges (and saved to csv for neptune loading). we will actually
    create 3 files : 2 vertex files (movies, actors) and 1 edge file (actor-movie relationship). separate vertex
    files allows us to control the neptune loading better.

    """

    # (0) read in movies+actors file
    data_dir = config.LOCAL_DATA_DIR

    # table is denormalized (pivoted on actors, 4 columns)
    #df_movies_pvt = pd.read_csv("{}{}{}".format(data_dir, os.sep, "imdb_top_1000.csv"))
    df_movies_pvt = s3_to_df("imdb_top_1000.csv", "csv")

    cols = ['Series_Title', 'Released_Year', 'Director', 'Star1', 'Star2', 'Star3', 'Star4']
    cols_map = {'Series_Title': 'name', 'Released_Year': 'year', 'Director': 'director',
                      'Star1': 'actor_1', 'Star2': 'actor_2', 'Star3': 'actor_3', 'Star4': 'actor_4'}

    # grab pertinent columns and rename
    df_movies_pvt = df_movies_pvt[cols].rename(columns = cols_map)
    # create id column (guid)
    df_movies_pvt['mid'] = [str(uuid.uuid4()) for _ in range(len(df_movies_pvt))]

    # (1) movies (vertex)
    df_movies = df_movies_pvt[['mid', 'name', 'year', 'director']].copy()
    df_movies['label'] = "movie"

    # (2) directors (vertex)
    df_directors = pd.DataFrame(df_movies['director'].unique(), columns=['director'])
    # create id column (guid)
    df_directors['did'] = [str(uuid.uuid4()) for _ in range(len(df_directors))]
    df_directors['label'] = "director"

    # (3) movie-director (edge)
    # update movies with director id, create new df
    df_movie_directors = pd.merge(df_movies, df_directors, on='director')[['mid', 'did', 'director']]
    # create id column (guid)
    df_movie_directors['mdid'] = [str(uuid.uuid4()) for _ in range(len(df_movie_directors))]
    df_movie_directors['label'] = "directed_by"

    # (4.a) actors (pivoted)
    df_actors_pivot = df_movies_pvt[['mid', 'actor_1', 'actor_2', 'actor_3', 'actor_4']]

    # (4.b) movie-actors (normalized, but has actors now duplicated)
    df_movie_actors = pd.melt(df_actors_pivot,
                        id_vars = ['mid'],
                        value_vars = ['actor_1', 'actor_2', 'actor_3', 'actor_4'],
                        var_name = 'actor',
                        value_name = 'name'
                        )
    # set to ordinal only (e.g. 1 instead of actor_1), used for debugging
    df_movie_actors['actor'] = df_movie_actors['actor'].str.split('_').str[1].astype(int)
    # create id column (guid)
    df_movie_actors['maid'] = [str(uuid.uuid4()) for _ in range(len(df_movie_actors))]
    # needed for edges file
    df_movie_actors['label'] = "acted_in"

    # (4.c) actors (normalized, unduplicated)
    df_actors = pd.DataFrame(df_movie_actors['name'].unique(), columns=['name'])
    # create id column (guid)
    df_actors['aid'] = [str(uuid.uuid4()) for _ in range(len(df_actors))]
    # needed for vertices file
    df_actors['label'] = "actor"

    # (5) update movie-actors with their aid's
    df_movie_actors = pd.merge(df_movie_actors, df_actors[['aid', 'name']], on='name')[['maid', 'mid', 'aid', 'name', 'label']]

    # test (join dfs back together to test vertexs/edges)
    """
    df_test = pd.merge(df_movies[['mid', 'name']].rename(columns = {'name': 'movie_name'}), df_movie_actors[['maid', 'mid', 'aid']], on = 'mid')
    df_test = pd.merge(df_actors[['aid', 'name']].rename(columns = {'name': 'actor_name'}), df_test, on = 'aid')
    #  look at one movie, and their actors
    print(df_test[df_test['movie_name'] == 'The Shawshank Redemption'])
    #  look at one actor, and their movies
    print(df_test[df_test['actor_name'] == 'Morgan Freeman'])
    df_test = pd.merge(df_movies[['mid', 'name']].rename(columns = {'name': 'movie_name'}), df_movie_directors[['mdid', 'mid', 'did']], on = 'mid')
    df_test = pd.merge(df_directors[['did', 'director']].rename(columns={'director': 'director_name'}), df_test, on='did')
    # look at one director and their movies
    print(df_test[df_test['director_name'] == 'Frank Darabont'])
    """

    # (6) save df's to csv (locally, so they can be viewed in the git project, and s3 where they will be loaded to Neptune)

    # movies (vertex)
    df_csv = (df_movies[['mid', 'label', 'name', 'year']].
              rename(columns = {'mid': '~id', 'label': '~label', 'name': 'name:String', 'year': 'year:String'}).copy())
    if save_to_local:
        df_to_local_fs(df_csv, "vertices--movie.csv")
    if save_to_s3:
        df_to_s3(df_csv, "vertices--movie.csv", "graph-csv/vertices")

    # directors (vertex)
    df_csv = (df_directors[['did', 'label', 'director']].
              rename(columns = {'did': '~id', 'label': '~label', 'director': 'name:String'}).copy())
    if save_to_local:
        df_to_local_fs(df_csv, "vertices--director.csv")
    if save_to_s3:
        df_to_s3(df_csv, "vertices--director.csv", "graph-csv/vertices")

    # actors (vertex)
    df_csv = (df_actors[['aid', 'label', 'name']].
              rename(columns = {'aid': '~id', 'label': '~label', 'name': 'name:String'}).copy())
    if save_to_local:
        df_to_local_fs(df_csv, "vertices--actor.csv")
    if save_to_s3:
        df_to_s3(df_csv, "vertices--actor.csv", "graph-csv/vertices")

    # actor-movie (edge)
    df_csv = (df_movie_actors[['maid', 'aid', 'mid', 'label']].
              rename(columns = {'maid': '~id', 'aid': '~from', 'mid': '~to', 'label': '~label'}).copy())
    if save_to_local:
        df_to_local_fs(df_csv, "edges--actor-movie.csv")
    if save_to_s3:
        df_to_s3(df_csv, "edges--actor-movie.csv", "graph-csv/edges")

    # movie-director (edge)
    df_csv = (df_movie_directors[['mdid', 'mid', 'did', 'label']].
              rename(columns = {'mdid': '~id', 'mid': '~from', 'did': '~to', 'label': '~label'}).copy())
    if save_to_local:
        df_to_local_fs(df_csv, "edges--movie-director.csv")
    if save_to_s3:
        df_to_s3(df_csv, "edges--movie-director.csv", "graph-csv/edges")


def load_data(obj_type):

    # TODO loader needs wrapped in an asynchronous call/thread that checks on status

    # makes connection over http
    neptune = nu.NeptuneHTTPConnection()

    # load data from s3
    if obj_type == "vertices":
        neptune.load_csv("graph-csv/vertices")
    elif obj_type == "edges":
        neptune.load_csv("graph-csv/edges")
    else:
        pass


if __name__ == "__main__":

    tasks = ['clean', 'process', 'load_vertices', 'load_edges', 'analyze', 'summarize']
    task = sys.argv[1].lower()
    if task not in tasks:
        print("arg must be in [{}]".format(", ".join(tasks)))

    if task == "clean":
        clean_graph()
    elif task == "process":
        process_data()
    elif task == "load_vertices":
        load_data("vertices")
    elif task == "load_edges":
        load_data("edges")
    elif task == "analyze":
        analyze_graph()
    elif task == "summarize":
        summarize_graph()


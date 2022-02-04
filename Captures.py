from binascii import a2b_base64
from calendar import IllegalWeekdayError
from pyspark.sql.types import MapType, StringType, IntegerType
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext

from random import randrange


class Board():

    @staticmethod
    def make_board():

        locs = []
        for a in list("abcdefgh"):
            for b in list("12345678"):
                locs.append(a+b)

        b = {l: None for l in locs}
        # Set the pieces on the start manually, sigh
        starts = [("a", "R"),("b", "N"),("c", "B"),("d", "Q"),("e", "K"),("f", "B"),("g", "N"),("h", "R")]
        for l, p in starts:
            b[l+"1"] = p
            b[l+"8"] = p.lower()

        for l in "abcdefgh":
            b[l+"2"] = "P"
            b[l+"7"] = "p"

        return b

    @staticmethod 
    def make_captures():
        # TODO: measure if this actually has a performance improvement
        return {p: dict() for p in "RNBKQPrnbkqp"}

        # return {p: {l+n: 0 for l in "abcdefgh" for n in "12345678"} for p in "RNBKQPrnbkqp"}

    def add_capture(self, location, piece):
        d = self.captures[piece]
        if location in d:
            self.captures[piece][location] += 1
        else:
            self.captures[piece][location] = 1



    def __init__(self):
        self.board = Board.make_board()
        self.captures = Board.make_captures()

    def get_square(self, location):
        return self.board[location]

    def set_square(self, location, piece):
        # print(f"    Set: {location}, {piece}")
        self.board[location] = piece

    def get_board(self):
        return self.board

    def handle_move(self, move, color):

        if "{" in move:
            raise ValueError('There are comments in this pgn.')

        # Get rid of checks
        if "+" in move or "#" in move:
            move = move[:-1]

        # End of game
        if "1-" in move or "-1" in move or "2-" in move:
            return

        # Move counter
        if "." in move:
            return

        # Castling
        elif "O-O" in move:

            # Short
            if len(move) == 3:
                if color == "white":
                    self.set_square("g1", "K")
                    self.set_square("f1", "R")
                else:
                    self.set_square("g8", "k")
                    self.set_square("f8", "r")           
            # Long
            else:
                if color == "white":
                    self.set_square("c1", "K")
                    self.set_square("d1", "R")
                else:
                    self.set_square("c8", "k")
                    self.set_square("d8", "r")

        # Promotions
        elif "=" in move:
            square, p = list(move.split("="))
            if color == "black":
                p = p.lower()
            self.set_square(square, p)

        # capture
        elif "x" in move:
            taker, taken_loc = list(move.split("x"))

            taken = self.get_square(taken_loc)
            # print(f"Taken: {taken}")
            self.add_capture(taken_loc, taken)


            # Pawn took
            if taker in "abcdefgh":
                taker = "P" if color == "white" else "p"
            else:
                taker = taker if color == "white" else taker.lower()

            # The zero parses the edge case where there are two of the same piece that can make the move
            self.set_square(taken_loc, taker[0])

        # Normal move
        else:
            # Pawn move
            if len(move) == 2:
                if color == "white":
                    self.set_square(move, "P")
                else:
                    self.set_square(move, "p")

            else:
                p = move[0]
                loc = move[-2:]
                p = p if color=="white" else p.lower()

                self.set_square(loc, p)
                
            
    def read_game(self, line):
        moves = line.split()
        counter = 0
        for move in moves:
            move = move.strip()
            self.handle_move(move, "white" if counter !=2 else "black")
            counter += 1
            if counter == 3:
                counter = 0

        return self.captures


def convert_udf(line):
    b = Board()
    res = None
    try:
        return b.read_game(line.strip())
    except:
        return None


conversion_udf = udf(convert_udf,  MapType(
    StringType(), MapType(StringType(), IntegerType())))


def custom_combine(a, b):
    """
    Given two dicts, we need to combine all the pieces and location counts
    The input shape is <PieceName <Location, Count>>
    """

    if type(a) is Row:
        a = a[0]
    if type(b) is Row:
        b = b[0]

    res = {p: {l+n: 0 for l in "abcdefgh" for n in "12345678"}
           for p in "RNBKQPrnbkqp"}

    if a is not None and b is not None:
        if len(a) != 12 or len(b) != 12:
            return res

    if a is None or b is None:
        return res

    for p in a:
        for l in a[p]:
            res[p][l] += a[p][l]
    for p in b:
        for l in b[p]:
            res[p][l] += b[p][l]
    return res


sc = SparkContext(appName="Chess vis")
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")

print("---------------------------------------------------------------------")


# Reading input data
print('Reading input data')
rdd = sc.textFile(
    "/user/s1811967/chess/lichess_db_standard_rated_2015-10.pgn.bz2")

print('Creating Dataframe')
# Grab only the games and filter out games with evaluations
rdd2 = rdd.filter(lambda x: "1. " in x and "{" not in x)
# Bit of a dumb hack to easily convert to DataFrame
df = rdd2.map(lambda x: (randrange(start=0, stop=10), x)).toDF()
df2 = df.select(col('_1').alias('k'), col('_2').alias('pgn'))
df2.show(truncate=35)
print("Using a UDF to calculate all capture locations per piece")
df3 = df2.select(df2.k, conversion_udf(df2.pgn).alias("captures"))
df3.show(truncate=35)

print("Doing reduce")

rdd = df3.rdd

rdd = rdd.reduceByKey(custom_combine)
rdd = rdd.map(lambda x: (0, x[1]))
print("Done the big boy")
rdd = rdd.reduceByKey(custom_combine)
print(rdd.take(1))
res = rdd.toDF()
res = res.select(col("_2").alias("result"))
print(res.head())
s = udf(lambda x: str(x))
res = res.select(s(res.result))
res.write.text("results")
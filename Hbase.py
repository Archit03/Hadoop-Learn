from starbase import Connection

# Connect to the HBase REST server
c = Connection("127.0.0.1", "8000")

# Create a table named 'ratings' or use an existing one if it exists
ratings = c.table('ratings')

if ratings.exists():
    print("Dropping existing ratings table")
    ratings.drop()

# Create a new 'ratings' table with a 'rating' column family
ratings.create('rating')

print("Parsing the ml-100k ratings data...\n")
ratingFile = open("D:\Downloads\ml-100k\ml-100k/u.data", "r")

# Check if the table was created successfully
if ratings.exists():
    batch = ratings.batch()

    for line in ratingFile:
        # Split the line into userID, movieID, rating, and timestamp
        (userID, movieID, rating, timestamp) = line.split()
        # Update the batch with the rating information for each user and movie
        batch.update(userID, {'rating': {movieID: rating}})

    ratingFile.close()

    print("Committing ratings data to HBase via REST service...\n")
    batch.commit(finalize=True)

    # Retrieve and print some ratings
    print("Get some ratings for user ID 1:\n")
    print(ratings.fetch("1"))

    print("Get some ratings for user ID 33:\n")
    print(ratings.fetch("33"))

    # Drop the 'ratings' table
    print("Dropping ratings table...\n")
    ratings.drop()
else:
    print("Failed to create the 'ratings' table.")

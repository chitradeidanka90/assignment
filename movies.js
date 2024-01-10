const { Client } = require('pg');
const fs = require('fs');
const csv = require('csv-parser');

// PostgreSQL database connection configuration
const client = new Client({
    user: 'movies_wp8t_user',
    host: 'dpg-cmeoaren7f5s73fv8pfg-a.render.com',
    database: 'movies_wp8t',
    password: 'uXW7dvsW5UqQjZMGTgA2nYrFXzvS0uiK',
    port: 5432,
  });

// Function to create tables
async function createTables() {
  await client.connect();

  // Create movie table
  await client.query(`
    CREATE TABLE IF NOT EXISTS movies (
      movie_id SERIAL PRIMARY KEY,
      title VARCHAR(255),
      duration INT,
      year_of_release INT,
      director VARCHAR(255),
      genre VARCHAR(255)
    );
  `);

  // Create rating table
  await client.query(`
    CREATE TABLE IF NOT EXISTS ratings (
      rating_id SERIAL PRIMARY KEY,
      movie_id INT,
      rater_id INT,
      rating INT,
      FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
    );
  `);
}

// Function to import CSV data into tables
async function importData() {
  // Import movies data
  const moviesData = fs.createReadStream('movies.csv').pipe(csv());
  for await (const row of moviesData) {
    await client.query(`
      INSERT INTO movies (title, duration, year_of_release, director, genre)
      VALUES ($1, $2, $3, $4, $5)
    `, [row.title, row.duration, row.year_of_release, row.director, row.genre]);
  }

  // Import ratings data
  const ratingsData = fs.createReadStream('ratings.csv').pipe(csv());
  for await (const row of ratingsData) {
    await client.query(`
      INSERT INTO ratings (movie_id, rater_id, rating)
      VALUES ($1, $2, $3)
    `, [row.movie_id, row.rater_id, row.rating]);
  }
}

// Function to perform insights and analyses
async function analyzeData() {
  // Perform your analyses as before...

  // Finally, close the database connection
  await client.end();
}

// Run the script
async function main() {
  await createTables();
  await importData();
  await analyzeData();
}

main();

const fs = require("fs");
const fetch = require("node-fetch");
const parse = require("csv-parse/lib/sync");
const stringify = require("csv-stringify");

const date = new Date();
const month = date.getMonth();
const year = date.getFullYear();

// TODO: read from s3
const LOCAL_CSV_FILE = "./snowdepth.csv";

// fetch and parse snowfall data
// requesting a single year gets you the season
// eg. requesting 2017 on 10/1 gets you only the fall, not the previous
// winter, though much of it was in 2017
async function getSnowfallData() {
  const res = await fetch(
    `http://www.uvm.edu/~empact/data/gendateplot.php3?table=SummitStation&title=Mount+Mansfield+Summit+Station&xskip=7&xparam=Date&yparam=Depth&year%5B%5D=${month <
    8
      ? year - 1
      : year}&width=800&height=600&smooth=0&csv=1&totals=0`
  );
  const text = await res.text();
  // text() returns markup with data inside `pre` tag
  const dataAsString = text.match(/(?![<pre>])(.|\n)*(?=<\/pre>)/g);
  const parsedData = parse(dataAsString, { columns: true });
  return parsedData;
}

// async friendly readFile
function readFile(filename, encoding = "utf8") {
  return new Promise((fulfill, reject) => {
    fs.readFile(filename, encoding, (err, res) => {
      if (err) reject(err);
      else fulfill(res);
    });
  });
}

// read local csv
async function readCSV() {
  const csvText = await readFile(LOCAL_CSV_FILE, "utf8");
  const parsed = parse(csvText, { columns: true });
  return parsed;
}

// munge data from ski-vt into shape of our csv
function munge(sourceData) {
  const seasonLabel =
    month > 8 // season begins september 1
      ? `${year}-${year + 1}`
      : `${year - 1}-${year}`;

  return sourceData.reduce(
    (season, reading) => {
      const newDateLabel = reading.Date
        .split("-")
        .slice(1)
        .map(n => parseInt(n, 10)) // remove zero padding
        .join("/");

      return {
        ...season,
        [newDateLabel]: reading.Depth ? parseInt(reading.Depth, 10) : 0
      };
    },
    { year: seasonLabel }
  );
}

function stringifyObjectAsCsv(object) {
  const options = {
    header: true,
    columns: Object.keys(object[0])
  };
  return new Promise((fulfill, reject) => {
    stringify(object, options, (err, res) => {
      if (err) reject(err);
      else fulfill(res);
    });
  });
}

async function start() {
  const currentSeasonData = munge(await getSnowfallData());
  const historicalData = await readCSV();
  const historicalLatestData = historicalData[historicalData.length - 1];

  const historicalLatestYear = historicalLatestData.year;
  const currentSeasonDataYear = currentSeasonData.year;

  // only write new csv if data is stale
  if (
    JSON.stringify(historicalLatestYear) !==
    JSON.stringify(currentSeasonDataYear)
  ) {
    historicalData.push(currentSeasonData);
    const data = await stringifyObjectAsCsv(historicalData);
    fs.writeFile(LOCAL_CSV_FILE, data, err => {
      if (err) console.log(err);
    });
  }
}

start();

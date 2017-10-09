const fetch = require("node-fetch");
const parse = require("csv-parse/lib/sync");
const stringify = require("csv-stringify");
const AWS = require("aws-sdk");

// set up AWS stuff
AWS.config.update({ region: "us-east-1" });
const s3 = new AWS.S3();
var s3Params = {
  Bucket: "matthewparrilla.com",
  Key: "snowDepth.csv"
};

const date = new Date();
const month = date.getMonth();
const year = date.getFullYear();

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

// async friendly getItem from DynamoDB
function getLastSavedDate() {
  return new Promise((fulfill, reject) => {
    dynamodb.getItem(lastUpdatedParams, (err, { Item: { value: { S } } }) => {
      if (err) reject(err);
      else fulfill(S);
    });
  });
}

// determine if ski-vt has fresh data by diffing last updated from dynamo
async function updateRequired() {
  // TODO: write this
}

function getSnowDepthFromS3() {
  return new Promise((fulfill, reject) => {
    s3.getObject(s3Params, (err, { Body }) => {
      if (err) reject(err);
      else fulfill(Body);
    });
  });
}

// read local csv
async function readCSV() {
  const csvText = await getSnowDepthFromS3();
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

function writeSnowDepthToS3(data) {
  const writeParams = {
    ...s3Params,
    Body: data,
    ACL: "public-read"
  };
  s3.putObject(writeParams, console.log);
}

async function main() {
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
    writeSnowDepthToS3(data);
  }
}

exports.handler = main;

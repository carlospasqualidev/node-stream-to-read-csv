import fs from "fs";
import { Transform, Writable, pipeline } from "stream";
import { promisify } from "util";
import readline from "readline";

function createObjectReference(header) {
  const obj = {};
  for (let i = 0; i < header.length; i++) {
    const key = header[i];
    obj[key] = null;
  }

  return obj;
}

function transformLine(line, delimiter) {
  let data = "";
  const parsedLine = [];

  let quoteCount = 0;

  for (let i = 0; i < line.length; i++) {
    if (line[i] !== delimiter && line[i] !== '"') data += line[i];

    if (line[i] === '"') quoteCount++;

    if (quoteCount === 1 && line[i] !== '"') {
      continue;
    }

    if (quoteCount === 2) {
      parsedLine.push(data);
      quoteCount = 0;
      data = "";
      continue;
    }

    if (data && (line[i] === delimiter || i === line.length - 1)) {
      parsedLine.push(data);
      data = "";
    }
  }

  return parsedLine;
}

async function readCSV(filePath, delimiter, stream) {
  //#region CONFIGURE STREAMS

  //#region READLINE
  const pipelineAsync = promisify(pipeline);
  const readLines = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  //#endregion

  //#region TRANSFORM STREAM
  let isHeader = true;
  let objectReference = {};

  const transformCSV = new Transform({
    transform(line, encoding, callback) {
      const transformedLine = transformLine(line.toString(), delimiter);

      if (isHeader) {
        isHeader = false;
        objectReference = createObjectReference(transformedLine);
        return callback();
      }

      const object = { ...objectReference };

      for (let i = 0; i < transformedLine.length; i++) {
        const key = Object.keys(object)[i];
        object[key] = transformedLine[i];
      }
      callback(null, JSON.stringify(object) + "\n");
    },
  });
  //#endregion

  //#endregion
  console.time("Pipeline");
  await pipelineAsync(readLines, transformCSV, stream)
    .then(() => {
      console.log("Pipeline finished");
      console.timeEnd("Pipeline");
    })
    .catch((err) => {
      console.error(err);
    });
}

//#region WRITE STREAM
function getLine(execute) {
  return new Writable({
    write(chunk, encoding, callback) {
      execute(JSON.parse(chunk));
      callback();
    },
  });
}

//#endregion

const files = [
  "Operações Jazz.xlsm - CLIENTE.csv",
  "Operações Jazz.xlsm - MODELO.csv",
  "cord_19_embeddings_2022-06-02.csv",
];

await readCSV(
  files[0],
  ",",
  getLine((data) => console.log(data))
);

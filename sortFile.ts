import { PathLike, promises } from "fs";
const fsp = promises; // use promise fs methods for async

const EOL = "\r\n"; // end of line character (differs between operating systems) use \n for macos and use \r\n for windows
const TEMP_FILE_PREFIX = "-temp-"; // prefix for temporary segment files
const TEMP_FILE_EXTENSION = ".txt"; // extension for temporary segment files
const MAX_ASCII_CHAR = String.fromCharCode(127); // highest possible ascii character for the finding smallest function

class SortFile {
  maxFileSizeBytes: number;
  numberOfLinesPerSegment: number;
  lineSizeBytes: number;

  constructor(
    maxFileSizeBytes: number,
    numberOfLinesPerSegment: number,
    lineSizeBytes: number
  ) {
    this.maxFileSizeBytes = maxFileSizeBytes;
    this.numberOfLinesPerSegment = numberOfLinesPerSegment;
    this.lineSizeBytes = lineSizeBytes;
  }

  // main sorting method
  async Sort(inFilename: string, outFilename: PathLike) {
    await this.validateInput(inFilename, outFilename);
    const sortedSegments = await this.sortSegments(inFilename);
    await this.mergeSegments(sortedSegments, outFilename);
  }

  // validate input file and output directory with the requirements
  async validateInput(inFilename, outFilename) {
    await this.checkFileExists(inFilename);
    await this.checkFileSizeLimit(inFilename);
    await this.validateFileContents(inFilename);
    await this.checkOutputDirectory(outFilename);
  }

  // check if file exists
  async checkFileExists(filename: string) {
    try {
      await fsp.access(filename);
    } catch (error) {
      throw new Error(`File does not exist: ${filename}`);
    }
  }

  // check if file size is within the specified limit
  async checkFileSizeLimit(filename) {
    const stats = await fsp.stat(filename);
    if (stats.size > this.maxFileSizeBytes) {
      throw new Error(
        `File size exceeds maxFileSizeBytes: ${stats.size} > ${this.maxFileSizeBytes}`
      );
    }
  }

  // validate file contents (line length and count)
  async validateFileContents(filename) {
    const fileHandle = await fsp.open(filename, "r");
    try {
      await this.checkLineLengthAndCount(fileHandle);
    } finally {
      await fileHandle.close();
    }
  }

  // check each line's length and total line count
  async checkLineLengthAndCount(fileHandle: promises.FileHandle) {
    let bytesRead = 0;
    let buffer = Buffer.alloc(this.lineSizeBytes);
    let lineCount = 0;

    while (true) {
      const { bytesRead: readBytes, buffer: readBuffer } =
        await fileHandle.read(buffer, 0, this.lineSizeBytes, bytesRead);
      if (readBytes === 0) break; // end of file

      const line = readBuffer.toString("ascii").trim();
      if (line.length + EOL.length !== this.lineSizeBytes) {
        throw new Error(
          `Not all lines have length equal to ${this.lineSizeBytes}`
        );
      }

      lineCount++;
      bytesRead += readBytes;
    }

    // check if total line count is divisible by numberOfLinesPerSegment
    if (lineCount % this.numberOfLinesPerSegment !== 0) {
      throw new Error(
        `Number of lines (${lineCount}) is not divisible by numberOfLinesPerSegment (${this.numberOfLinesPerSegment})`
      );
    }
  }

  // check if output directory exists
  async checkOutputDirectory(outFilename: string) {
    try {
      await fsp.access(outFilename);
    } catch (error) {
      throw new Error(`Output directory does not exist: ${outFilename}`);
    }
  }

  // sort file into segments
  async sortSegments(inFilename: string) {
    const segments: string[] = [];
    let segmentCounter = 0;
    let bytesRead = 0;
    const fileNameNoEnd = inFilename.slice(0, -4); // remove .txt from filename

    const fileHandle = await fsp.open(inFilename, "r");
    try {
      while (true) {
        const lines = await this.readSegment(fileHandle, bytesRead);
        if (lines.length === 0) break; // end of file

        const tempFilePath = await this.writeSortedSegment(
          lines,
          segmentCounter,
          fileNameNoEnd
        );
        segments.push(tempFilePath);
        segmentCounter++;
        bytesRead += lines.length * this.lineSizeBytes;
      }
    } finally {
      await fileHandle.close();
    }
    return segments;
  }

  // read a segment of lines from the main file
  async readSegment(fileHandle: promises.FileHandle, startPosition: number) {
    const lines: string[] = [];
    let buffer = Buffer.alloc(this.lineSizeBytes);

    for (let i = 0; i < this.numberOfLinesPerSegment; i++) {
      const { bytesRead, buffer: readBuffer } = await fileHandle.read(
        buffer,
        0,
        this.lineSizeBytes,
        startPosition + i * this.lineSizeBytes
      );
      if (bytesRead === 0) break; // End of file

      const line = readBuffer.toString("utf8").trim();
      lines.push(line);
    }
    return lines;
  }

  // write sorted segment to a temporary file
  async writeSortedSegment(
    lines: string[],
    segmentCounter: number,
    fileNameNoEnd: string
  ) {
    lines.sort();
    // use the original filename in the temp file name to allow sorting multiple different files simultaneously
    const tempFilePath = `${fileNameNoEnd}${TEMP_FILE_PREFIX}${segmentCounter}${TEMP_FILE_EXTENSION}`;
    await fsp.writeFile(tempFilePath, lines.join(EOL) + EOL, "ascii");

    return tempFilePath;
  }

  // merge sorted segments into final output file
  async mergeSegments(segments: string[], outFilename: PathLike) {
    const fileHandles = await Promise.all(
      segments.map((segment) => fsp.open(segment, "r"))
    );
    const outFile = await fsp.open(outFilename, "w");

    try {
      await this.performMerge(fileHandles, outFile);
    } finally {
      await this.cleanupFiles(fileHandles, outFile, segments);
    }
  }

  // perform the actual merge of segments
  async performMerge(
    fileHandles: promises.FileHandle[],
    outFile: promises.FileHandle
  ) {
    let lines = await this.readInitialLines(fileHandles);
    let positions = new Array(fileHandles.length).fill(this.lineSizeBytes);

    while (lines.some((line) => line !== "")) {
      const { minLine, minIndex } = this.findMinLine(lines);
      await this.writeLineToOutput(outFile, minLine);
      await this.readNextLine(
        fileHandles[minIndex],
        minIndex,
        lines,
        positions
      );
    }
  }

  // read initial lines from all segments
  async readInitialLines(fileHandles: promises.FileHandle[]) {
    const readResults = await Promise.all(
      fileHandles.map((handle) =>
        handle.read(Buffer.alloc(this.lineSizeBytes), 0, this.lineSizeBytes, 0)
      )
    );
    return readResults.map((result) => result.buffer.toString("ascii").trim());
  }

  // find the lexicographically(alphabetically) smallest line among all segments
  findMinLine(lines: string[]) {
    const minLine = lines.reduce(
      (min, current) => (current !== "" && current < min ? current : min),
      MAX_ASCII_CHAR
    );
    const minIndex = lines.indexOf(minLine);
    return { minLine, minIndex };
  }

  // write a line to the output file
  async writeLineToOutput(outFile: promises.FileHandle, line: string) {
    await outFile.write(
      Buffer.from(line.padEnd(this.lineSizeBytes - EOL.length) + EOL, "ascii")
    );
  }

  // read the next line from a specific segment
  async readNextLine(
    fileHandle: promises.FileHandle,
    index: string | number,
    lines: string[],
    positions: number[]
  ) {
    const nextReadResult = await fileHandle.read(
      Buffer.alloc(this.lineSizeBytes),
      0,
      this.lineSizeBytes,
      positions[index]
    );
    positions[index] += this.lineSizeBytes;

    if (nextReadResult.bytesRead === 0) {
      lines[index] = ""; // mark segment as exhausted
    } else {
      lines[index] = nextReadResult.buffer.toString("ascii").trim();
    }
  }

  // close all file handles and delete temporary files
  async cleanupFiles(
    fileHandles: promises.FileHandle[],
    outFile: promises.FileHandle,
    segments: string[]
  ) {
    await Promise.all(fileHandles.map((handle) => handle.close()));
    await outFile.close();
    await Promise.all(segments.map((segment) => fsp.unlink(segment)));
  }
}

//----------------------running-----------------------

// file paths
const OUTPUT_FILE_1 = "TestFiles/output1.txt";
const OUTPUT_FILE_2 = "TestFiles/output2.txt";

const FIVE_PER_ROW_FILE = "TestFiles/5InALine.txt";
const SIX_PER_ROW_FILE_1 = "TestFiles/6InALine.txt";
const SIX_PER_ROW_FILE_2 = "TestFiles/V2_6InALine.txt";

// sorter configs
const MAX_SIZE = 800000;
const CHUNK_LINES = 1000;
const LINE_SIZE = 7;

const sorter = new SortFile(MAX_SIZE, CHUNK_LINES, LINE_SIZE);
// sort one file
async function sortSingleFile() {
  try {
    await sorter.Sort(FIVE_PER_ROW_FILE, OUTPUT_FILE_1);
    console.log("Single file sorting complete");
  } catch (error) {
    console.error("Error during single file sorting:", error);
  }
}
// sort two files at once with one SortFile instance
async function sortTwoFilesConcurrently() {
  try {
    const sortPromises = [
      sorter.Sort(SIX_PER_ROW_FILE_1, OUTPUT_FILE_1),
      sorter.Sort(SIX_PER_ROW_FILE_2, OUTPUT_FILE_2),
    ];

    await Promise.all(sortPromises);
    console.log("Concurrent sorting of two files complete");
  } catch (error) {
    console.error("Error during concurrent file sorting:", error);
  }
}

sortTwoFilesConcurrently(); //uncomment if you want to check two files at once
//sortSingleFile(); //uncomment if you want to check one file

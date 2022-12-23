-- Load the input
inputFile = LOAD 'hdfs:///user/varsha/inputs' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Filter out the first two lines
ranked = RANK inputFile;
ranked_lines = FILTER ranked BY (rank_inputFile>2);

-- Group lines by name
grpd = GROUP ranked_lines by name;

-- Count the number of lines by each character
total_count = FOREACH grpd GENERATE $0 as name, COUNT($1) as no_of_lines;
result = ORDER total_count BY no_of_lines DESC;

-- Remove output Folder
rmf hdfs:///user/varsha/PigProjectOutput;

-- Store the Result
STORE result INTO 'hdfs:///user/varsha/PigProjectOutput';

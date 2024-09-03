CREATE NODE TABLE person (ID INT64,fName STRING,gender INT64,isStudent BOOL,isWorker BOOL,age INT64,eyeSight DOUBLE,birthdate DATE,registerTime TIMESTAMP,lastJobDuration INTERVAL,workedHours INT64[],usedNames STRING[],courseScoresPerTerm INT64[][],grades INT64[4],height FLOAT,u UUID, PRIMARY KEY(ID));
CREATE NODE TABLE organisation (ID INT64,name STRING,orgCode INT64,mark DOUBLE,score INT64,history STRING,licenseValidInterval INTERVAL,rating DOUBLE,state STRUCT(revenue INT16, location STRING[], stock STRUCT(price INT64[], volume INT64)),info UNION(price FLOAT, movein DATE, note STRING), PRIMARY KEY(ID));
CREATE REL TABLE studyAt (FROM person TO organisation, year INT64,places STRING[],length INT16,level INT8,code UINT64,temprature UINT32,ulength UINT16,ulevel UINT8,hugedata INT128,MANY_ONE);
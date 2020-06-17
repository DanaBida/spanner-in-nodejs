const express = require('express');
const PORT = 3001;
const app = express();

app.listen(PORT, () => {
  console.log(`Listening on port: ${PORT}`);
});

/////////////////////////////////////////SPENNER/////////////////////////////////////////////

const { Spanner } = require('@google-cloud/spanner');

//we need to define the emulator host before the creation of the Spanner project!
process.env.SPANNER_EMULATOR_HOST = 'localhost:9010';

const projectId = 'spannerproject';
const spanner = new Spanner({ projectId: projectId });

const instanceId = 'apollo-test',
  databaseId = 'apollo-db-test';
let instance, database;

const instanceConfig = {
  config: 'emulator-config',
  nodes: 1,
};

const createSpannerInstance = async () => {
  console.log('create spanner instance');
  try {
    const [createdInstance, operation] = await spanner.createInstance(instanceId, instanceConfig);

    console.log(`Waiting for operation on ${createdInstance.id} to complete...`);
    await operation.promise(); //await complete event

    instance = createdInstance;
    console.log('Instance created successfully.');
  } catch (err) {
    console.error('ERROR:', err);
  }
};

const createSpannerDatabase = async () => {
  console.log('create spanner db');
  try {
    const [createdDatabase, operation] = await instance.createDatabase(databaseId);

    console.log(`Waiting for operation on ${createdDatabase.id} to complete...`);
    await operation.promise(); //await complete event

    database = createdDatabase;
    console.log(`Created database ${databaseId} on instance ${instanceId}.`);
  } catch (err) {
    console.error('ERROR:', err);
  }
};

const createSpannerTable = async () => {
  console.log('create table');
  try {
    const schema = `CREATE TABLE Singers (
      SingerId    INT64 NOT NULL,
      FirstName   STRING(1024),
      LastName    STRING(1024)
    ) PRIMARY KEY (SingerId)`;

    const [_, operation] = await database.createTable(schema);

    console.log(`Waiting for operation on ${database.id} to complete...`);
    await operation.promise();

    console.log(`table created successfully`);
  } catch (err) {
    console.error('ERROR:', err);
  }
};

//data manipulation language (DML) is a computer programming language used for inserting, deleting, and updating data in a database.
//DML statements may not be performed in single-use transactions, to avoid replay.
// insert data using DML in a read-write transaction - use runUpdate() method to execute a DML statement.
const insertSpannerTable = async () => {
  // add check of existence
  console.log('insert to spanner db table - use transaction');
  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error('ERROR:', err);
      return;
    }
    try {
      const query = {
        sql: `INSERT Singers (SingerId, FirstName, LastName) VALUES
        (12, 'Melissa', 'Garcia'),
        (13, 'Russell', 'Morales'),
        (14, 'Jacqueline', 'Long'),
        (15, 'Dylan', 'Shaw')`,
      };
      const [rowCount] = await transaction.runUpdate(query);
      await transaction.commit();
      console.log(`${rowCount} records inserted.`);
    } catch (err) {
      console.error('ERROR:', err);
      //TODO: think about retry
    }
  });
};

const updateSpannerTable = async () => {
  console.log('update spanner db table');

  const query = {
    sql: "UPDATE Singers SET FirstName = 'dana' WHERE SingerId = 12",
  };

  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error('ERROR:', err);
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`Successfully updated ${rowCount} record.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
    }
  });
};

const deleteSpannerTable = async () => {
  console.log('update spanner db table');

  const query = {
    sql: "DELETE FROM Singers WHERE FirstName = 'dana'",
  };

  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error(err);
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`Successfully deleted ${rowCount} record.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
    }
  });
};

//Use database.run() to run SQL query.
const selectSpannerTable = async () => {
  console.log('select from spanner db table');
  /*
    // The SQL query string can contain parameter placeholders. A parameter
     * // placeholder consists of '@' followed by the parameter name.
     * //-
     * const query = {
     *   sql: 'SELECT * FROM Singers WHERE name = @name',
     *   params: {
     *     name: 'Eddie Wilson'
     *   }
     */
  const query = {
    sql: 'SELECT * FROM Singers',
  };

  try {
    //rows type - By default rows are an Array of values in the form of objects containing name and value properties.
    //If you prefer plain objects, you can use the {@link Row#toJSON} method.
    //NOTE: If you have duplicate field names only the last field will be present.
    const [rows] = await database.run(query);
    rows.forEach((row) => {
      const json = row.toJSON(); //return in string with JSON formate
      console.log(`SingerId: ${json.SingerId}, FirstName: ${json.FirstName}, LastName: ${json.LastName}`);
    });
  } catch (err) {
    console.error('ERROR:', err);
  }
};

app.get('/create-instance', async (req, res) => {
  await createSpannerInstance();
  return res.json();
});

app.get('/create-db', async (req, res) => {
  await createSpannerDatabase();
  return res.json();
});

app.get('/create-table', async (req, res) => {
  await createSpannerTable();
  return res.json();
});

app.get('/insert', async (req, res) => {
  await insertSpannerTable();
  return res.json();
});

app.get('/select', async (req, res) => {
  await selectSpannerTable();
  return res.json();
});

app.get('/update', async (req, res) => {
  await updateSpannerTable();
  return res.json();
});

app.get('/delete', async (req, res) => {
  await deleteSpannerTable();
  return res.json();
});

/////////////////////////////////////////KNEX/////////////////////////////////////////////

const knex = require('knex')({ client: 'mysql', useNullAsDefault: true }); //useNullAsDefault - undefined keys are replaced with NULL instead of DEFAULT

/*`CREATE TABLE Singers (
        SingerId    INT64 NOT NULL,
        FirstName   STRING(1024),
        LastName    STRING(1024)
      ) PRIMARY KEY (SingerId)` 
create table `Singers` (`SingerId` int, `FirstName` varchar(1024), `LastName` varchar(1024));
alter table`Singers` add primary key`Singers_pkey`(`SingerId`)*/
const createKnexTable = async () => {
  console.log('create knex db table');

  let query = knex.schema
    .createTable('Singers', (table) => {
      table.integer('SingerId').primary();
      table.string('FirstName', 1024);
      table.string('LastName', 1024);
    })
    .toSQL();

  console.log('query createTable created');

  // const query = knex.schema
  //   .withSchema('create')
  //   .createTable('Singers', function (table) {
  //     table.integer('SingerId', 64).primary();
  //     table.string('FirstName', 1024);
  //     table.string('LastName', 1024);
  //     // table.binary('SingerInfo', 1024);
  //   })
  //   .toSQL();

  // Creates a database
  const [database1, operation] = await instance.createDatabase(databaseId, query);

  console.log(`Waiting for operation on ${database1.id} to complete...`);
  await operation.promise();

  console.log(`Created database ${databaseId} on instance ${instanceId}.`);
  database = database1;
};

/*INSERT Singers (SingerId, FirstName, LastName) VALUES
        (12, 'Melissa', 'Garcia'),
        (13, 'Russell', 'Morales'),
        (14, 'Jacqueline', 'Long'),
        (15, 'Dylan', 'Shaw')
insert into `Singers` (`FirstName`, `LastName`, `SingerId`) values ('Marc', 'Richards', 1), ('Catalina', 'Smith', 2), ('Alice', 'Trentor', 3), ('Lea', 'Martin', 4) */
const insertKnexTable = async () => {
  console.log('inset to knex db table');

  const singers = [
    { SingerId: 1, FirstName: 'Marc', LastName: 'Richards' },
    { SingerId: 2, FirstName: 'Catalina', LastName: 'Smith' },
    { SingerId: 3, FirstName: 'Alice', LastName: 'Trentor' },
    { SingerId: 4, FirstName: 'Lea', LastName: 'Martin' },
  ];
  let query = knex('Singers').insert(singers).toSQL();
  // let query = knex.insert(singers).into('Singers').toSQL();

  console.log('insert query: ', query);

  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error(err);
      // database.close();
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`${rowCount} records inserted.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
      // database.close();
    }
    // finally {
    // Close the database when finished.
    // database.close();
    // }
  });
};

/* SELECT * FROM Singers'
select * from `Singers`*/
const selectKnexTable = async () => {
  console.log('select from knex db table');

  let query = knex.from('Singers').select('*').toSQL();
  // let query = knex.from('Singers').select('FirstName', 'LastName').toSQL();

  // Queries rows from the Albums table
  try {
    const [rows] = await database.run(query);

    rows.forEach((row) => {
      const json = row.toJSON();
      console.log(`SingerId: ${json.SingerId}, FirstName: ${json.FirstName}, LastName: ${json.LastName}`);
    });
  } catch (err) {
    console.error('ERROR:', err);
    // database.close();
  } finally {
    // Close the database when finished.
    // await database.close();
  }
};

/*UPDATE Singers SET FirstName = 'dana' WHERE SingerId = 12
update `Singers` set `FirstName` = 'dana' where `SingerId` = 12*/
const updateKnexTable = async (updateRawsCondition, updatedFieldsInRaws) => {
  console.log('update knex db table');

  // let query = knex('Singers').where(updateRawsCondition).update(updatedFieldsInRaws).toSQL();
  let query = knex('Singers').where('SingerId', '=', 12).update(updatedFieldsInRaws).toSQL();

  // let query = knex('Singers')
  //   .where((builder) => {
  //     filteringConditions.forEach((condition) => {
  //       builder.where(...condition);
  //     });
  //   })
  //   .update(fields)
  //   .toSQL();

  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error(err);
      // database.close();
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`Successfully updated ${rowCount} record.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
      // database.close();
    }
    // finally {
    // Close the database when finished.
    // database.close();
    // }
  });
};

/*DELETE FROM Singers WHERE FirstName = 'dana'
delete from `Singers` where `FirstName` = 'dana' */
const deleteKnexTable = async (deleteRawsCondition) => {
  console.log('delete from knex db table');

  // let query = knex('Singers').where('SingerId', 12).del().toSQL();
  // let query = knex('Singers').where(knex.raw('? = ?', ['SingerId', 12]));
  let query = knex('Singers').where(deleteRawsCondition).delete().toSQL();

  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error(err);
      // database.close();
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`Successfully deleted ${rowCount} record.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
      // database.close();
    }
    // finally {
    // Close the database when finished.
    // database.close();
    // }
  });
};

app.get('/create-knex', async (req, res) => {
  await createKnexTable();
  return res.json();
  // knex.destroy();
});

app.get('/insert-knex', async (req, res) => {
  await insertKnexTable();
  return res.json();
  // knex.destroy();
});

app.get('/select-knex', async (req, res) => {
  await selectKnexTable();
  return res.json();
  // knex.destroy();
});

app.get('/update-knex', async (req, res) => {
  await updateKnexTable({ SingerId: 12 }, { FirstName: 'dana' });
  return res.json();
  // knex.destroy();
});

app.get('/delete-knex', async (req, res) => {
  await deleteKnexTable({ FirstName: 'dana' });
  return res.json();
  // knex.destroy();
});

app.get('/test', async (req, res) => {
  let query = knex('coords')
    .insert([{ x: 20 }, { y: 30 }, { x: 10, y: 20 }])
    .toSQL();
  console.log(query);
  return res.json();
  // knex.destroy();
});

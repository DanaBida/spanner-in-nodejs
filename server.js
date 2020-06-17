// APIs: select, insert, update, delete

const express = require('express');
const PORT = 3001;
const app = express();

app.listen(PORT, () => {
  console.log(`Listening on port: ${PORT}`);
});

/////////////////////////////////////////SPENNER/////////////////////////////////////////////

const { Spanner } = require('@google-cloud/spanner');

const projectId = 'spannerproject';
const config = {
  config: 'emulator-config',
  nodes: 1,
};

process.env.SPANNER_EMULATOR_HOST = 'localhost:9010';

const spanner = new Spanner({ projectId: projectId });
const instanceId = 'apollo-test',
  databaseId = 'apollo-db-test';
let instance, database;
// const instance = spanner.instance(instanceId);
// const database = instance.database(databaseId);
// console.log('database: ', database);
// console.log('instance: ', instance);

const createSpannerInstance = async () => {
  console.log('createSpannerInstance');
  try {
    const [instance1, operation] = await spanner.createInstance(instanceId, config);

    console.log(`Waiting for operation on ${instance1.id} to complete...`);
    await operation.promise();

    instance = instance1;
    console.log('Instance created successfully.');
  } catch (err) {
    console.error('ERROR:', err);
  }
};

const createSpannerDatabase = async () => {
  console.log('create spanner db');
  try {
    const [database1, operation] = await instance.createDatabase(databaseId);

    console.log(`Waiting for operation on ${database1.id} to complete...`);
    await operation.promise();

    database = database1;
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

//TODO: try to run without transaction
const insertSpannerTable = async () => {
  console.log('insert to spanner db table - use transaction');
  //TODO: check if could use await
  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error(err);
      //TODO: think about retry
      database.close();
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate({
        sql: `INSERT Singers (SingerId, FirstName, LastName) VALUES
        (12, 'Melissa', 'Garcia'),
        (13, 'Russell', 'Morales'),
        (14, 'Jacqueline', 'Long'),
        (15, 'Dylan', 'Shaw')`,
      });
      // const [rowCount] = await transaction.runUpdate(query);
      console.log(`${rowCount} records inserted.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
      database.close();
    }
    // finally {
    // Close the database when finished.
    // database.close();
    // }
  });
};
//DML statements may not be performed in single-use transactions, to avoid replay.
const insertSpannerTableWithoutTransaction = async () => {
  console.log('insert to spanner db table - without transaction');

  try {
    const query = {
      sql: `INSERT Singers (SingerId, FirstName, LastName) VALUES
      (12, 'Melissa', 'Garcia'),
      (13, 'Russell', 'Morales'),
      (14, 'Jacqueline', 'Long'),
      (15, 'Dylan', 'Shaw')`,
    };
    const [rows] = await database.run(query);
    //TODO: check rows type - By default rows are an Array of values in the form of objects containing name and value properties.
    //If you prefer plain objects, you can use the {@link Row#toJSON} method.
    //NOTE: If you have duplicate field names only the last field will be present.
    rows.forEach((row) => {
      const json = row.toJSON(); //return plain objects
      console.log(`SingerId: ${json.SingerId}, FirstName: ${json.FirstName}, LastName: ${json.LastName}`);
    });
  } catch (err) {
    console.error('ERROR:', err);
    database.close();
  }
};

const selectSpannerTable = async () => {
  console.log('select from spanner db table');
  const query = {
    sql: 'SELECT * FROM Singers',
  };

  // Queries rows from the Albums table
  try {
    const [rows] = await database.run(query);
    //TODO: check rows type
    rows.forEach((row) => {
      const json = row.toJSON(); //return in string with JSON formate
      console.log(`SingerId: ${json.SingerId}, FirstName: ${json.FirstName}, LastName: ${json.LastName}`);
    });
  } catch (err) {
    console.error('ERROR:', err);
    database.close();
  } finally {
    // Close the database when finished.
    // await database.close();
  }
};

const updateSpannerTable = async () => {
  console.log('update spanner db table');

  const query = {
    sql: "UPDATE Singers SET FirstName = 'dana' WHERE SingerId = 12",
  };

  database.runTransaction(async (err, transaction) => {
    if (err) {
      console.error(err);
      database.close();
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`Successfully updated ${rowCount} record.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
      database.close();
    }
    // finally {
    // Close the database when finished.
    // database.close();
    // }
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
      database.close();
      return;
    }
    try {
      const [rowCount] = await transaction.runUpdate(query);
      console.log(`Successfully deleted ${rowCount} record.`);
      await transaction.commit();
    } catch (err) {
      console.error('ERROR:', err);
      database.close();
    }
    // finally {
    // Close the database when finished.
    // database.close();
    // }
  });
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

const mongodb = require('mongodb');
const async = require('async');
const customerAddressDataArray = require('./m3-customer-address-data.json');
const customerDataArray = require('./m3-customer-data.json');
const MongoClient = mongodb.MongoClient;
const url = "mongodb://localhost:27017/customers";

/*
tasks array consist of functions , every function has same objective
the only difference is the range over which they operate to acheive their
objective. Note that each function has its own unique range and no 2 functions
have any range in common as all of them are executed in parallel
*/
let tasks = [];

let customers = [];

/*
process.argv[2] specifies how many items we want each task to transfer
thus if input is 50, it means each task will transfer 50 records (if possible) into database
thus together 20 such tasks will run in parallel in order to transfer 1000 records
*/
const numberOfItems = parseInt(process.argv[2], 10);
let start=0, end=0;


MongoClient.connect(url,
(error, dbo)=>
{
  if(error) return process.exit(1);
  db = dbo.db('customers');
  customerDataArray.forEach((value, ind)=>{
    customerDataArray[ind] = Object.assign(value, customerAddressDataArray[ind]);
    //creating each task such that it handles numberOfItems and insert them into db collection
    if(ind % numberOfItems == 0)
    {
      start=ind;
      end= ind + numberOfItems;
      tasks.push(()=>{
        db.collection("customers").insert(customerDataArray.slice(start, end), (err, results)=>{
          if(err) return process.exit(1);
          //console.log(results);
        });
      });//end of each task
    }
  });//end of forEach loop
  console.log("tasks created : " + tasks.length);
  async.parallel(tasks, (err, res) => {
    if (err){ console.log("Error performing tasks");
      return process.exit(1);
    }
    console.log("Successfully finished data transfer into the collection customers\n Tasks required : "+tasks.length + ", ideal count of elements handled by each task : "+numberOfItems);

    db.close();
  });
});

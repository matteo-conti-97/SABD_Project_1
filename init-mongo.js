use("results");
db.createCollection("hello_world");
db.createCollection("query1");
db.createCollection("query2");
db.createCollection("query3");
db.createCollection("process_time");
db.createUser({
    user: "spark_user",
    pwd: "spark_password",
    roles: [{
        role :"readWrite",
        db: "results"
    }],
});
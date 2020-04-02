const express=require('express');
const url=require('url');
const { schema }=require('./CustomSchema');
const graphqlHTTP=require('express-graphql');
const morgan=require('morgan');
const app=express();
app.use(morgan('dev'));

app.use('/graphql/navhistory',graphqlHTTP({
    schema: schema,
    graphiql: true,
    customFormatErrorFn: (err) => {
        msg=err.message.replace('Unexpected error value: "','').replace('"','');
    
        return ({ message:msg})
      }
    

})
);


//listen for requests
app.listen(4000,
    function(){
        console.log('now listensing to port '+4000);
    });
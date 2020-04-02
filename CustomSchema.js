const {graphql,buildSchema}=require('graphql');
const {GraphQLObjectType , GraphQLString,GraphQLSchema, GraphQLList,
    GraphQLFloat, GraphQLInt,GraphQLBoolean}=graphql
const _= require('underscore');
const lodash = require('lodash');
const { makeExecutableSchema } = require("graphql-tools");
const {GetData}=require('./DataFetchFurtherOptimized');
const GraphQLDate=require('graphql-iso-date');



const typeDefs=`
    type NAVDetails{
    FUNDID: String
    NAVEFFECTIVE_DATE: String
    NAV: String
    NAV_CHANGE_VALUE: String
    AS_OF_DATE_ORDER: String
    CURRENCY_CODE: String
    DAILY_NAV_CHANGE_PERCENTAGE: String
    APPLICATION_PRICE: Float
    REDEMPTION_PRICE: Float
    NAV_DATE_STD: String
    REINVESTED_NAV: String
    FUND_SHCL_CNT_LNG_RNK: Int

    }
    type PaginationDetails{
        totalrecords: Int
        currentpagerecordcount: Int
        totalpages:  Int
        perpagelimit: Int
        hasnextpage: Boolean
        
    }

    

    type NAV{
        NAV : [NAVDetails]
        PageInfo: PaginationDetails
    }



    type Query {

        NAVHistoryDetails(begindate: String,enddate: String,countrycode: String,languagecode: String,
            fundid: String,shareclasscode: String,pageno: Int): NAV
    }
`;



const resolvers={
    Query: {
        NAVHistoryDetails: async(parent,args, context, info)=>{
                 return await GetData(args)
        }
   
}

 };


const schema=makeExecutableSchema({ typeDefs,resolvers });

module.exports={
    schema: schema
}

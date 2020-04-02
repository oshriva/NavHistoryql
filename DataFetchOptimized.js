const IgniteClient = require('apache-ignite-client');
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const ENDPOINT='172.30.197.6';// '172.30.197.6';'127.0.0.1','10.65.107.71'
const CACHE_NAME='NAVHistoryCache';
const _ = require('underscore');
const {numformatfn}=require('./NumberFormatFn');
const dataForge=require('data-forge');
const dateFormat = require('dateformat');
const pagecount=1000;



const objprpchk=function(obj,attr){
    
    if(obj!=undefined){
        if(obj.hasOwnProperty(attr)){
                return obj[attr]
        }
        else{
              return null
        }
    }else{
        return null
    }
};

const func2=async function(QueryString,queryparams,Canonical){
    const starttime = new Date();
    const igniteClient =await new IgniteClient();

    
    try {
        console.log("Func2 StartTime for canonical: "+Canonical+" "+starttime.toJSON());
        
        await igniteClient.connect(new IgniteClientConfiguration(ENDPOINT));
        const cache =igniteClient.getCache(CACHE_NAME);
        let query=new SqlFieldsQuery(QueryString).setArgs(...queryparams);
        query.setIncludeFieldNames(true);
        const cursor =await cache.query(query);
       
        console.log("CursorCreated for canonical: "+Canonical+" "+new Date().toJSON());
        const data = await cursor.getAll();
        console.log("DataFetched for canonical: "+Canonical+" "+new Date().toJSON());        
        const fields=cursor.getFieldNames();
        console.log("Field names fetched  for canonical "+Canonical+" "+new Date().toJSON());
        
        const res_data=await  (new dataForge.DataFrame({
            columnNames:fields,
            rows:data
                })).toJSON();
        
        console.log("Columns and Data Mapped for canonical "+Canonical+" "+new Date().toJSON());
        
        let res_data_final=JSON.parse(res_data);
        let res_data_sorted;
        let pageobj={};
        
        
        if (Canonical=="NumberandDateFormat" || Canonical=="NAVstartrowid" ){
             res_data_sorted=res_data_final;
          }
        else if(res_data_final.length>0){
           
             res_data_sorted=await (_.sortBy(res_data_final,'FUND_SHCL_CNT_LNG_RNK')).slice(0,pagecount);
             
             pageobj.hasnextpage= res_data_final.length>pagecount ? true : false;
             pageobj.pagestartrowid=(res_data_sorted[0]).FUND_SHCL_CNT_LNG_RNK;
             pageobj.pageendrowid=(res_data_sorted[res_data_sorted.length-1]).FUND_SHCL_CNT_LNG_RNK;
             
        }
        else{
            res_data_sorted=[];
        }
              
        console.log("Data Frame is converted to JS Native Object for canonical "+Canonical+" "+new Date().toJSON());
        

        const endtime = new Date();
        console.log("Func2 execution for canonical "+Canonical+" completed in  "+(endtime.getTime()-starttime.getTime())+" milliseconds");

        return  Promise.resolve([res_data_sorted,pageobj]);



    }
   

finally {

    console.log("Func2 execution Completed for canonical "+Canonical+" "+new Date().toJSON());
    igniteClient.disconnect();    

}
}

const DataFetch=async function FetchData(args){
    

    try {

        const starttime = new Date();
        console.log("Data Generation Started at "+starttime.toJSON());
        
        const queryNAVHistory=`SELECT DISTINCT 

        FUNDID,
        NAVEFFECTIVE_DATE,
        NAV,
        NAV_CHANGE_VALUE,
        AS_OF_DATE_ORDER,
        CURRENCY_CODE,
        DAILY_NAV_CHANGE_PERCENTAGE,
        APPLICATION_PRICE,
        REDEMPTION_PRICE,
        NAV_DATE_STD,
        REINVESTED_NAV,
        FUND_SHCL_CNT_LNG_RNK
        FROM Pie.NAVHistory
        WHERE 
        
        FUNDID=?
        AND SHARE_CLASS_CODE=?
        AND AS_OF_DATE>=? AND AS_OF_DATE<=?
        AND FUND_SHCL_CNT_LNG_RNK BETWEEN ? AND ? 
        
        `;

        
        const queryNAVstartRowid=`SELECT MIN(FUND_SHCL_CNT_LNG_RNK) AS STARTROWID,COUNT(1) AS CNT_REC
        FROM Pie.NAVHistory
        WHERE 
        FUNDID=?
        AND SHARE_CLASS_CODE=?
        AND AS_OF_DATE>=? AND AS_OF_DATE<=?
        
        `
        ;

        const queryNumberandDateFormat=`select 
        LANGUAGE_CODE,
        COUNTRY_CODE,
        LABEL_CATEGORY,
        FMT_CODE,
        FMT_DCML_SEP,
        FMT_THOUSAND_SEP,
        FMT_SYMBL,
        FMT_RNDNG,
        FMT_DATE,
        FMT_PRFX_SUFX
        
        
        from 
      
        PIE.NumberandDateFormat 
     
        where COUNTRY_CODE=? and LANGUAGE_CODE=? AND LABEL_CATEGORY IN (?,?,?,?) 
        `;


     let results_initial=await func2(queryNAVstartRowid,
        [args.fundid,args.shareclasscode,args.begindate,args.enddate,
        ],"NAVstartrowid");
     
    const cnt_rec=results_initial[0][0]['CNT_REC'];
    const totpages=Math.ceil(cnt_rec/pagecount);
    console.log(results_initial[0][0]);

    const initialrowid=results_initial[0][0]['STARTROWID'] ;
    const startrowid=args.pageno==1 ? initialrowid : initialrowid+(pagecount*(args.pageno-1));

    let results=await Promise.all(
        [
        func2(queryNAVHistory,[args.fundid,args.shareclasscode,args.begindate,args.enddate,
                startrowid,
                startrowid+pagecount+10],"NAVHistory"),
        func2(queryNumberandDateFormat,[args.countrycode,args.languagecode,'PRICE','CURRENCY_SYMBOL','NAV_CHANGE_PERCENT','AS_OF_DATE'],"NumberandDateFormat"),
       ])
     
         
         
         let NAVHistorydata=await results[0][0];
         let pagedetails=await results[0][1];
         let numberdateformatdata=await results[1][0];
         
         let NAVHistorydataFinal=[];
        
        
        
        if (NAVHistorydata.length>0 && numberdateformatdata.length>0){

            

            const PRICE= _.find(numberdateformatdata,
                {COUNTRY_CODE:args.COUNTRY_CODE,LANGUAGE_CODE:args.LANGUAGE_CODE,
                    LABEL_CATEGORY: 'PRICE'});

         
          const NAV_CHANGE_PERCENT= _.find(numberdateformatdata,
                        {COUNTRY_CODE:args.countrycode,LANGUAGE_CODE:args.languagecode,
                            LABEL_CATEGORY: 'NAV_CHANGE_PERCENT'});

                            
           const CURRENCY_SYMBOL= _.find(numberdateformatdata,
                                {COUNTRY_CODE:args.countrycode,LANGUAGE_CODE:args.languagecode,
                                    LABEL_CATEGORY: 'CURRENCY_SYMBOL'});

            const FRMT_DATE= _.find(numberdateformatdata,
                                        {COUNTRY_CODE:args.countrycode,LANGUAGE_CODE:args.languagecode,
                                            LABEL_CATEGORY: 'AS_OF_DATE'});


        

             await NAVHistorydata.map((elem)=>{

            let navobj=elem;

            
                                              
            const nveffdate = (new Date((elem.NAVEFFECTIVE_DATE)));

            const nveffdatefinal=dateFormat(nveffdate,(FRMT_DATE.FMT_DATE).toLowerCase());

            elem.NAVEFFECTIVE_DATE=nveffdatefinal;
        

            navobj.NAV_CHANGE_VALUE=numformatfn(navobj.NAV_CHANGE_VALUE,
                objprpchk(PRICE,'FMT_DCML_SEP'),objprpchk(PRICE,'FMT_THOUSAND_SEP'),
                    objprpchk(CURRENCY_SYMBOL,'FMT_SYMBL'),objprpchk(PRICE,'FMT_RNDNG'),
                        objprpchk('CURRENCY_SYMBOL','FMT_PRFX_SUFX'));
            
                        
            
            
            navobj.NAV=numformatfn(navobj.NAV,objprpchk(PRICE,'FMT_DCML_SEP'),
                                objprpchk(PRICE,'FMT_THOUSAND_SEP'),
                                    objprpchk(CURRENCY_SYMBOL,'FMT_SYMBL'),objprpchk(PRICE,'FMT_RNDNG'),
                                        objprpchk(CURRENCY_SYMBOL,'FMT_PRFX_SUFX'));
            
                                        
                                        
            navobj.DAILY_NAV_CHANGE_PERCENTAGE=numformatfn(navobj.DAILY_NAV_CHANGE_PERCENTAGE,
                objprpchk(NAV_CHANGE_PERCENT,'FMT_DCML_SEP'),
                objprpchk(NAV_CHANGE_PERCENT,'FMT_THOUSAND_SEP'),
                objprpchk(NAV_CHANGE_PERCENT,'FMT_SYMBL'),objprpchk(NAV_CHANGE_PERCENT,'FMT_RNDNG'),
                objprpchk(NAV_CHANGE_PERCENT,'FMT_PRFX_SUFX')
            );
            

        
            navobj.REINVESTED_NAV= numformatfn(navobj.REINVESTED_NAV,objprpchk(PRICE,'FMT_DCML_SEP'),
                objprpchk(PRICE,'FMT_THOUSAND_SEP'),
                    objprpchk( CURRENCY_SYMBOL,'FMT_SYMBL'),objprpchk(PRICE,'FMT_RNDNG'),null);

                     NAVHistorydataFinal.push(navobj);
                   
                    //Promise.resolve("Done");

                    
            
         });
        }
        else if(NAVHistorydata.length>0){
            NAVHistorydataFinal=NAVHistorydata;
        }
        else{
            throw new Error("No Data Exists for given input");
        }

        
        objfinal={};
        objfinal.NAV=NAVHistorydataFinal;
        objfinal.PageInfo=
        {
        totalrecords:cnt_rec,
        currentpagerecordcount:objfinal.NAV.length ,
        totalpages:totpages,
        perpagelimit:pagecount,
        hasnextpage:pagedetails['hasnextpage']};

        const endtime = new Date();
        console.log("Data Generation completed in  "+(endtime.getTime()-starttime.getTime())+" milliseconds");

        return  await Promise.resolve(objfinal);
        
    }      
                        
catch (err) {
    
    console.log(err.message)
    throw err.message
}

finally {
   
    console.log("Data Generation completed at "+new Date().toJSON());
    
    

}

}

//DataFetch({begindate:"2014-04-04",enddate:"2014-04-05",fundid:"256",shareclasscode: "A",countrycode:"BE",
//languagecode:"fr_BE"}).then((res)=>console.log(res)).catch((err)=>console.log(err));


module.exports={
    
    GetData: DataFetch
}




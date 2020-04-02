
const wordlefttrim=function(word,rmvchar){
    const arr=word.split("");
    let finalword;
    let counter=0;
    for(i=0;i<=arr.length;i++){
        if(arr[i]==rmvchar){
            counter=counter+1;
        }
        else{
            break;
        }
    }

    return word.substring(counter)

}

const wordrighttrim=function(word,rmvchar){
    const arr=(word.split("")).reverse();
    let finalword;
    let counter=0;    
    for(i=0;i<=arr.length;i++){
        if(arr[i]==rmvchar){
            counter=counter+1;
        }
        else{
            break;
        }
    }

    return word.substring(0,word.length-counter)

}

const numformatfn=function(inElementValue,FMT_DCML_SEP,FMT_THOUSAND_SEP,FMT_SYMBL,FMT_RNDNG,FMT_PRFX_SUFX){
    if(inElementValue==null){
        inElementValue="";
    }else{
        inElementValue=inElementValue.toString();
    }


    if(FMT_DCML_SEP==null){
        FMT_DCML_SEP="";
    }


    if(FMT_THOUSAND_SEP==null){
        FMT_THOUSAND_SEP="";
    }

    if(FMT_SYMBL==null){
        FMT_SYMBL="";
    }
   
    if(FMT_PRFX_SUFX==null){
        FMT_PRFX_SUFX="";
    }

    



    let negative="N";
    let pv_out_put_str=" ";
    let finalString=" ";
    let finalstringafterrounding=" ";
    let finalstringafterseperator=" ";
    let beforeDelimiter2=" ";
    let Decimal_Separator_String,beforeDelimiter,afterDelimiter;
    let Decimal_Separator_String2,beforeDelimiter3,roundString;
    let beforeRounding,afterRounding,AfterRoundString,strlength,loopcount;
    let x=1;
    let PV_DATA_COUNT=1;
    let PV_ElementValue,V_AfterRoundString,PV_ZERO_CHK;

    if(inElementValue != "-" && inElementValue.substring(0,1) == "-"){
        negative="Y";
        PV_ElementValue=inElementValue.substring(1)

    }
    else 
    {
        if(inElementValue.length==0){
            formatedValue=null
            return formatedValue;
        } 
        else{
            PV_ElementValue=inElementValue;
        }
    };
    
    if (FMT_RNDNG!=null){
        
           roundString=parseFloat(PV_ElementValue);
           
           V_AfterRoundString=roundString.toFixed(FMT_RNDNG);
           if( V_AfterRoundString==0){
             PV_ZERO_CHK="YES";   
            }
            AfterRoundString= V_AfterRoundString.toString();
            beforeRounding=AfterRoundString.substring(0,AfterRoundString.indexOf("."));
            afterRounding=(AfterRoundString.substring(AfterRoundString.indexOf(".")+1)).
                           substring(0,FMT_RNDNG);
           finalString=wordrighttrim((beforeRounding+"."+afterRounding),".");
    }
    else{
        finalString=PV_ElementValue;
    }

    finalstringafterrounding=finalString;
    
    
    if(FMT_DCML_SEP.length>0 && finalString.indexOf(".")!=-1){
        
         beforeDelimiter=finalString.substring(0,finalString.indexOf("."));
         
        if (beforeDelimiter.length<3){
            finalString=finalString.replace(".",FMT_DCML_SEP);
        }
        else{
            if (FMT_THOUSAND_SEP.length>0){
                
                 afterDelimiter=finalString.substring(finalString.indexOf(".")+1);
  
                 loopcount=Math.floor((beforeDelimiter.length/3));
                 
                
                 beforeDelimiter2=beforeDelimiter;

                while(1==1){
                        if (x>loopcount){
                            break;
                        }
                        
                        pv_out_put_str=FMT_THOUSAND_SEP+
                                  beforeDelimiter2.substring(beforeDelimiter2.length-3)+pv_out_put_str.trimRight();
                                 
                                  beforeDelimiter2=beforeDelimiter2.substring(0,beforeDelimiter2.length-3);
                                  beforeDelimiter3=beforeDelimiter2;
                        x=x+1;  
                        
               
                }
                Decimal_Separator_String = beforeDelimiter3+pv_out_put_str+FMT_DCML_SEP+afterDelimiter;
                finalString = Decimal_Separator_String;
                finalString=wordlefttrim(finalString,FMT_THOUSAND_SEP);
            }
            else{
                finalString=finalString.replace(".",FMT_DCML_SEP);
            }
        }
    } 
    else {
        
        if(finalString.indexOf(".")!=-1){
            beforeDelimiter = finalString.substring(0,finalString.indexOf("."));
            
        }else{
            beforeDelimiter=finalString;
        }
        if (beforeDelimiter.length<3){
            finalString = finalString;
        }else{
            if (FMT_THOUSAND_SEP.length>0){
                
                if (finalstring.indexOf(".")!=-1){
                    beforeDelimiter=finalString.substring(0,finalstring.indexOf("."));
                    afterDelimiter=finalString.substring(finalstring.indexOf(".")+1);
                    loopcount=Math.floor(beforeDelimiter.length/3);
                    beforeDelimiter2=beforeDelimiter;
               while(1==1){
                       if (x>loopcount){
                           break;
                       }
                pv_out_put_str=FMT_THOUSAND_SEP+
                                 beforeDelimiter2.substring(beforeDelimiter2.length-3)+pv_out_put_str.trimRight();
                                 beforeDelimiter2=beforeDelimiter2.substring(0,beforeDelimiter2.length-3);
                beforeDelimiter3=beforeDelimiter2;
                x=x+1;  
               }
               Decimal_Separator_String = beforeDelimiter3+pv_out_put_str+"."+afterDelimiter;
               finalString = Decimal_Separator_String;
               finalString=wordlefttrim(finalString, FMT_THOUSAND_SEP)
           }
           else{
                     beforeDelimiter=finalString;
                     loopcount=Math.floor(beforeDelimiter.length/3);
                     beforeDelimiter2=beforeDelimiter;
                     while(1==1){
                        if (x>loopcount){
                            break;
                        }
                    pv_out_put_str=FMT_THOUSAND_SEP+
                    beforeDelimiter2.substring(beforeDelimiter2.length-3)+pv_out_put_str.trimRight();
                    beforeDelimiter2=beforeDelimiter2.substring(0,beforeDelimiter2.length-3);
                    beforeDelimiter3=beforeDelimiter2;
                    x=x+1;  
                    }
                    Decimal_Separator_String = beforeDelimiter3+pv_out_put_str;
                    finalString = Decimal_Separator_String;
                    finalString=wordlefttrim(finalString,FMT_THOUSAND_SEP);
           }
        }
    }
}


    finalstringafterseperator=finalString;
    PV_DATA_COUNT = 0;
    formatedValue = finalString;
    
    if(negative=="Y"){
        if (FMT_SYMBL.length>0){
            if(FMT_PRFX_SUFX == "S"){
                formatedValue = "-" || formatedValue || FMT_SYMBL;
            }
            else if(FMT_PRFX_SUFX == "P"){
                formatedValue = FMT_SYMBL ||"-"|| formatedValue;
            }
            else {
                
                formatedValue = "-" || formatedValue;
            }
        }else{
            
            formatedValue = "-"+formatedValue;

        }
        if( PV_ZERO_CHK == 'YES'){
            formatedValue = wordlefttrim(formatedValue, "-");
        }

    }
    else{
        if(FMT_SYMBL.length>0){
            if(FMT_PRFX_SUFX == "S"){
                formatedValue = formatedValue+FMT_SYMBL;
            }
            else if(FMT_PRFX_SUFX == "P"){
                formatedValue = FMT_SYMBL+formatedValue;
            }
            
        } 
        else{
            formatedValue = formatedValue;
        }      
        

    }
    return formatedValue;
}



//console.log(numformatfn(-1234.1842,".",",",null,2,null));

module.exports={
    
    numformatfn: numformatfn
}

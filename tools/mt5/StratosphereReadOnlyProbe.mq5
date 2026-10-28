#property strict
#property version "1.00"
#property description "Read-only account, gold specifications and recent history diagnostic. No orders."

// No credentials, account login/name, balances, positions, trade imports or trade calls.
string Clean(string value)
{
   StringReplace(value,"\t"," ");
   StringReplace(value,"\r"," ");
   StringReplace(value,"\n"," ");
   return value;
}

void Field(int file,string key,string value)
{
   FileWrite(file,key,Clean(value));
}

void OnStart()
{
   string run="stratosphere-probe-"+IntegerToString((long)TimeGMT())+"-"+
              IntegerToString((long)GetTickCount64());
   if(!FolderCreate(run)) { Print("stratosphere: cannot create probe directory"); return; }
   int meta=FileOpen(run+"\\metadata.tsv",FILE_WRITE|FILE_CSV|FILE_ANSI,'\t',CP_UTF8);
   if(meta==INVALID_HANDLE) { Print("stratosphere: cannot create metadata"); return; }
   Field(meta,"schema","stratosphere.mt5.readonly.v1");
   Field(meta,"captured_utc_epoch",IntegerToString((long)TimeGMT()));
   Field(meta,"connected",IntegerToString(TerminalInfoInteger(TERMINAL_CONNECTED)));
   Field(meta,"terminal_build",IntegerToString(TerminalInfoInteger(TERMINAL_BUILD)));
   Field(meta,"account_mode",EnumToString((ENUM_ACCOUNT_TRADE_MODE)AccountInfoInteger(ACCOUNT_TRADE_MODE)));
   Field(meta,"company",AccountInfoString(ACCOUNT_COMPANY));
   Field(meta,"server",AccountInfoString(ACCOUNT_SERVER));
   Field(meta,"currency",AccountInfoString(ACCOUNT_CURRENCY));
   Field(meta,"leverage",IntegerToString(AccountInfoInteger(ACCOUNT_LEVERAGE)));
   Field(meta,"profile_verification","not_verified; demo does not establish Free Trial, phase or Swing");
   Field(meta,"time_semantics","raw MT5 server/chart times; historical UTC conversion not verified");
   Field(meta,"server_now_epoch",IntegerToString((long)TimeTradeServer()));
   Field(meta,"terminal_max_bars",IntegerToString(TerminalInfoInteger(TERMINAL_MAXBARS)));

   string symbol="";
   string only="";
   int candidates=0;
   for(int i=0;i<SymbolsTotal(false);i++)
   {
      string name=SymbolName(i,false);
      if(StringFind(name,"XAUUSD")>=0)
      {
         candidates++;
         only=name;
         Field(meta,"gold_candidate_"+IntegerToString(candidates),name);
         if(name=="XAUUSD") symbol=name;
      }
   }
   if(symbol=="" && candidates==1) symbol=only;
   Field(meta,"symbol",symbol);
   bool originally_selected=false;
   bool usable=false;
   if(symbol!="")
   {
      originally_selected=(bool)SymbolInfoInteger(symbol,SYMBOL_SELECT);
      usable=originally_selected || SymbolSelect(symbol,true);
   }
   if(usable)
   {
      Field(meta,"description",SymbolInfoString(symbol,SYMBOL_DESCRIPTION));
      Field(meta,"base_currency",SymbolInfoString(symbol,SYMBOL_CURRENCY_BASE));
      Field(meta,"profit_currency",SymbolInfoString(symbol,SYMBOL_CURRENCY_PROFIT));
      Field(meta,"margin_currency",SymbolInfoString(symbol,SYMBOL_CURRENCY_MARGIN));
      Field(meta,"calculation_mode",IntegerToString(SymbolInfoInteger(symbol,SYMBOL_TRADE_CALC_MODE)));
      Field(meta,"digits",IntegerToString(SymbolInfoInteger(symbol,SYMBOL_DIGITS)));
      Field(meta,"contract_size",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_TRADE_CONTRACT_SIZE),8));
      Field(meta,"tick_size",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_TRADE_TICK_SIZE),8));
      Field(meta,"tick_value",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_TRADE_TICK_VALUE),8));
      Field(meta,"volume_min",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_VOLUME_MIN),8));
      Field(meta,"volume_max",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_VOLUME_MAX),8));
      Field(meta,"volume_step",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_VOLUME_STEP),8));
      Field(meta,"swap_mode",IntegerToString(SymbolInfoInteger(symbol,SYMBOL_SWAP_MODE)));
      Field(meta,"swap_long_current",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_SWAP_LONG),8));
      Field(meta,"swap_short_current",DoubleToString(SymbolInfoDouble(symbol,SYMBOL_SWAP_SHORT),8));
      MqlTick tick;
      if(SymbolInfoTick(symbol,tick))
      {
         Field(meta,"quote_server_epoch_ms",IntegerToString(tick.time_msc));
         Field(meta,"quote_server_age_seconds",IntegerToString((long)TimeTradeServer()-(long)tick.time));
         Field(meta,"bid",DoubleToString(tick.bid,8));
         Field(meta,"ask",DoubleToString(tick.ask,8));
      }
      ENUM_TIMEFRAMES frames[]={PERIOD_M15,PERIOD_H1,PERIOD_H4,PERIOD_D1,PERIOD_W1};
      for(int j=0;j<ArraySize(frames) && !IsStopped();j++)
      {
         MqlRates rates[];
         ResetLastError();
         int count=CopyRates(symbol,frames[j],0,512,rates);
         int error=GetLastError();
         string prefix=EnumToString(frames[j])+".";
         Field(meta,prefix+"copy_count",IntegerToString(count));
         Field(meta,prefix+"copy_error",IntegerToString(error));
         Field(meta,prefix+"local_bars",IntegerToString(SeriesInfoInteger(symbol,frames[j],SERIES_BARS_COUNT)));
         Field(meta,prefix+"server_first_epoch",IntegerToString(SeriesInfoInteger(symbol,frames[j],SERIES_SERVER_FIRSTDATE)));
         Field(meta,prefix+"local_first_epoch",IntegerToString(SeriesInfoInteger(symbol,frames[j],SERIES_FIRSTDATE)));
         Field(meta,prefix+"synchronized",IntegerToString(SeriesInfoInteger(symbol,frames[j],SERIES_SYNCHRONIZED)));
         if(count>0)
         {
            int bars=FileOpen(run+"\\"+EnumToString(frames[j])+".tsv",FILE_WRITE|FILE_CSV|FILE_ANSI,'\t',CP_UTF8);
            if(bars==INVALID_HANDLE) { FileClose(meta); return; }
            FileWrite(bars,"server_epoch","open","high","low","close","tick_volume","spread","real_volume");
            for(int k=0;k<count;k++)
               FileWrite(bars,IntegerToString((long)rates[k].time),DoubleToString(rates[k].open,8),
                         DoubleToString(rates[k].high,8),DoubleToString(rates[k].low,8),
                         DoubleToString(rates[k].close,8),IntegerToString(rates[k].tick_volume),
                         IntegerToString(rates[k].spread),IntegerToString(rates[k].real_volume));
            FileClose(bars);
         }
      }
      if(!originally_selected) SymbolSelect(symbol,false);
   }
   Field(meta,"finished_utc_epoch",IntegerToString((long)TimeGMT()));
   FileClose(meta);
   if(IsStopped()) return;
   int done=FileOpen(run+"\\complete.txt",FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(done!=INVALID_HANDLE)
   {
      FileWriteString(done,"stratosphere.mt5.readonly.v1\n");
      FileClose(done);
      Print("stratosphere read-only probe complete: ",TerminalInfoString(TERMINAL_DATA_PATH),"\\MQL5\\Files\\",run);
   }
}

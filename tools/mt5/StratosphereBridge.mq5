#property strict
#property version "1.00"
#property description "Local typed data/order bridge. Trading disabled by default. No DLL/network imports."

input bool EnableTrading=false;
input bool AllowRealAccount=false;
const string ROOT="stratosphere-bridge\\";
const string SCHEMA="stratosphere.mt5.bridge/1";
// Build tooling injects the SHA-256 of the reviewed source with this placeholder unchanged.
const string EA_SHA256="SOURCE_SHA256";
int owner=INVALID_HANDLE;
long bound_login=0;
string bound_server="",bound_mode="",session="";
string keys[],values[],policy_keys[],policy_values[];

string Mode() { return AccountInfoInteger(ACCOUNT_TRADE_MODE)==ACCOUNT_TRADE_MODE_REAL ? "real" :
                      (AccountInfoInteger(ACCOUNT_TRADE_MODE)==ACCOUNT_TRADE_MODE_DEMO ? "demo" : "contest"); }
string Safe(string v) { StringReplace(v,"\t"," "); StringReplace(v,"\r"," "); StringReplace(v,"\n"," "); return v; }
void Put(int h,string k,string v) { FileWrite(h,k,Safe(v)); }
string Num(double v) { return DoubleToString(v,10); }
string Get(string key,bool policy=false)
{
   if(policy) { for(int i=0;i<ArraySize(policy_keys);i++) if(policy_keys[i]==key) return policy_values[i]; }
   else { for(int i=0;i<ArraySize(keys);i++) if(keys[i]==key) return values[i]; }
   return "";
}
bool Hex(string s,int length)
{
   if(StringLen(s)!=length) return false;
   for(int i=0;i<length;i++) if(StringFind("0123456789abcdef",StringSubstr(s,i,1))<0) return false;
   return true;
}
bool Numeric(string s,bool integer=false)
{
   if(StringLen(s)==0 || StringLen(s)>30) return false;
   int dots=0;
   for(int i=0;i<StringLen(s);i++)
   {
      string c=StringSubstr(s,i,1);
      if(c=="." && !integer) dots++;
      else if(StringFind("0123456789",c)<0) return false;
   }
   return dots<=1 && MathIsValidNumber(StringToDouble(s));
}
bool Load(string file,bool policy=false)
{
   int h=FileOpen(file,FILE_READ|FILE_CSV|FILE_ANSI,'\t',CP_UTF8);
   if(h==INVALID_HANDLE) return false;
   if(FileSize(h)>16384) { FileClose(h); return false; }
   string k[],v[];
   bool valid=true;
   while(!FileIsEnding(h))
   {
      string key=FileReadString(h);
      if(key=="" && FileIsEnding(h)) break;
      if(FileIsLineEnding(h)) { valid=false; break; }
      string value=FileReadString(h);
      if(!FileIsLineEnding(h) && !FileIsEnding(h)) { valid=false; break; }
      int n=ArraySize(k);
      if(n>=64) { valid=false; break; }
      for(int i=0;i<n;i++) if(k[i]==key) valid=false;
      if(!valid) break;
      ArrayResize(k,n+1); ArrayResize(v,n+1); k[n]=key; v[n]=value;
   }
   FileClose(h);
   if(!valid) return false;
   if(policy) { ArrayCopy(policy_keys,k); ArrayCopy(policy_values,v); ArrayResize(policy_keys,ArraySize(k)); ArrayResize(policy_values,ArraySize(v)); }
   else { ArrayCopy(keys,k); ArrayCopy(values,v); ArrayResize(keys,ArraySize(k)); ArrayResize(values,ArraySize(v)); }
   return true;
}
bool Bound()
{
   return AccountInfoInteger(ACCOUNT_LOGIN)==bound_login && AccountInfoString(ACCOUNT_SERVER)==bound_server && Mode()==bound_mode;
}
bool OwnedMagic(string magic,string digest)
{
   int h=FileOpen(ROOT+"sent\\"+magic+".tsv",FILE_READ|FILE_CSV|FILE_ANSI,'\t',CP_UTF8);
   if(h==INVALID_HANDLE) return false;
   string first=FileReadString(h),request=FileReadString(h);
   string second=FileReadString(h),approval=FileReadString(h);
   FileClose(h);
   return first=="request" && Hex(request,32) && second=="approval" && approval==digest;
}
void Header(int h,string id,string op)
{
   Put(h,"schema",SCHEMA); Put(h,"id",id); Put(h,"session",session); Put(h,"op",op);
   Put(h,"login",IntegerToString(bound_login)); Put(h,"server",bound_server); Put(h,"mode",bound_mode);
}
void Error(int h,string message) { Put(h,"status","error"); Put(h,"error",message); }

int OnInit()
{
   FolderCreate("stratosphere-bridge"); FolderCreate(ROOT+"requests"); FolderCreate(ROOT+"processing");
   FolderCreate(ROOT+"responses"); FolderCreate(ROOT+"approvals"); FolderCreate(ROOT+"sent");
   owner=FileOpen(ROOT+"ea-owner.lock",FILE_READ|FILE_WRITE|FILE_BIN);
   if(owner==INVALID_HANDLE) { Print("stratosphere: another EA owns this terminal bridge"); return INIT_FAILED; }
   bound_login=AccountInfoInteger(ACCOUNT_LOGIN); bound_server=AccountInfoString(ACCOUNT_SERVER); bound_mode=Mode();
   if(bound_login<=0 || bound_server=="") { FileClose(owner); owner=INVALID_HANDLE; return INIT_FAILED; }
   session=IntegerToString((long)TimeGMT())+"-"+IntegerToString((long)GetMicrosecondCount());
   int h=FileOpen(ROOT+"session.tmp",FILE_WRITE|FILE_CSV|FILE_ANSI,'\t',CP_UTF8);
   if(h==INVALID_HANDLE) return INIT_FAILED;
   Put(h,"schema",SCHEMA); Put(h,"session",session); Put(h,"login",IntegerToString(bound_login));
   Put(h,"server",bound_server); Put(h,"mode",bound_mode); Put(h,"ea_sha256",EA_SHA256);
   FileClose(h);
   if(!FileMove(ROOT+"session.tmp",0,ROOT+"session.tsv",FILE_REWRITE)) return INIT_FAILED;
   EventSetTimer(1);
   Print("stratosphere bridge ready; trading input=",EnableTrading,"; local folder=",TerminalInfoString(TERMINAL_DATA_PATH),"\\MQL5\\Files\\",ROOT);
   return INIT_SUCCEEDED;
}
void OnDeinit(const int reason) { EventKillTimer(); if(owner!=INVALID_HANDLE) FileClose(owner); }

bool SymbolReady(string symbol)
{
   if(StringLen(symbol)==0 || StringLen(symbol)>64) return false;
   for(int i=0;i<StringLen(symbol);i++) if(StringFind("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.#-",StringSubstr(symbol,i,1))<0) return false;
   return SymbolSelect(symbol,true);
}

bool Policy(string digest,string symbol,bool exit_only=false)
{
   if(!Hex(digest,64) || !Load(ROOT+"approvals\\"+digest+".tsv",true)) return false;
   if(Get("digest",true)!=digest || Get("schema_version",true)!="mt5-deployment/1" ||
      Get("login",true)!=IntegerToString(bound_login) || Get("server",true)!=bound_server ||
      Get("mode",true)!=bound_mode || Get("symbol",true)!=symbol || Get("ea_sha256",true)!=EA_SHA256 ||
      Get("currency",true)!=AccountInfoString(ACCOUNT_CURRENCY)) return false;
   if(!exit_only && (StringToInteger(Get("expires",true))<=(long)TimeGMT() || Get("revoked",true)=="1")) return false;
   string limits[]={"max_volume","max_order_risk","max_order_margin","max_spread","min_equity","cost_buffer"};
   for(int i=0;i<ArraySize(limits);i++)
      if(!Numeric(Get(limits[i],true)) || StringToDouble(Get(limits[i],true))<=0) return false;
   return true;
}

void Trade(int h,string op,string id)
{
   string symbol=Get("symbol"),digest=Get("approval");
   bool exit_only=op=="close";
   if(!Policy(digest,symbol,exit_only) || !SymbolReady(symbol)) { Error(h,"Exact native deployment policy missing/invalid"); return; }
   if(!Numeric(Get("magic"),true) || StringToInteger(Get("magic"))<=0) { Error(h,"Invalid order identity"); return; }
   if(!TerminalInfoInteger(TERMINAL_CONNECTED) || !Bound()) { Error(h,"Disconnected or changed account"); return; }
   if(op!="check" && (!EnableTrading || !MQLInfoInteger(MQL_TRADE_ALLOWED) ||
      !TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) || !AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) ||
      !AccountInfoInteger(ACCOUNT_TRADE_EXPERT) || (bound_mode=="real" && !AllowRealAccount) || bound_mode=="contest"))
      { Error(h,"MT5 trading disabled or account mode not explicitly enabled"); return; }
   MqlTick tick;
   if(!SymbolInfoTick(symbol,tick) || tick.ask<=0 || tick.bid<=0 ||
      (long)TimeTradeServer()*1000-tick.time_msc>5000 || (long)TimeTradeServer()*1000-tick.time_msc < -2000)
      { Error(h,"Stale/invalid executable quote"); return; }
   MqlTradeRequest request={}; MqlTradeResult result={}; MqlTradeCheckResult check={};
   request.action=TRADE_ACTION_DEAL; request.symbol=symbol; request.magic=(ulong)StringToInteger(Get("magic"));
   request.comment="st:"+StringSubstr(id,0,24); request.deviation=10;
   long filling=SymbolInfoInteger(symbol,SYMBOL_FILLING_MODE);
   if((filling & SYMBOL_FILLING_FOK)!=0) request.type_filling=ORDER_FILLING_FOK;
   else if((filling & SYMBOL_FILLING_IOC)!=0) request.type_filling=ORDER_FILLING_IOC;
   else { Error(h,"No supported immediate filling policy"); return; }
   if(exit_only)
   {
      if(!Numeric(Get("ticket"),true) || !PositionSelectByTicket((ulong)StringToInteger(Get("ticket"))) ||
         PositionGetString(POSITION_SYMBOL)!=symbol || PositionGetInteger(POSITION_MAGIC)!=(long)request.magic ||
         !OwnedMagic(Get("magic"),digest)) { Error(h,"Position is not journal-owned"); return; }
      request.position=(ulong)StringToInteger(Get("ticket")); request.volume=PositionGetDouble(POSITION_VOLUME);
      request.type=PositionGetInteger(POSITION_TYPE)==POSITION_TYPE_BUY ? ORDER_TYPE_SELL : ORDER_TYPE_BUY;
   }
   else
   {
      if(PositionsTotal()!=0 || OrdersTotal()!=0) { Error(h,"Account not flat; reinforcement/external exposure blocked"); return; }
      if(!Numeric(Get("volume")) || !Numeric(Get("sl")) || !Numeric(Get("tp")) ||
         (Get("side")!="buy" && Get("side")!="sell")) { Error(h,"Invalid order fields"); return; }
      request.volume=StringToDouble(Get("volume")); request.sl=StringToDouble(Get("sl")); request.tp=StringToDouble(Get("tp"));
      request.type=Get("side")=="buy" ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
      double step=SymbolInfoDouble(symbol,SYMBOL_VOLUME_STEP),size=SymbolInfoDouble(symbol,SYMBOL_TRADE_TICK_SIZE);
      if(step<=0 || size<=0 || request.volume<SymbolInfoDouble(symbol,SYMBOL_VOLUME_MIN) ||
         request.volume>SymbolInfoDouble(symbol,SYMBOL_VOLUME_MAX) || request.volume>StringToDouble(Get("max_volume",true)) ||
         MathAbs(request.volume/step-MathRound(request.volume/step))>0.000001 || request.sl<=0 ||
         MathAbs(request.sl/size-MathRound(request.sl/size))>0.000001 ||
         (request.tp>0 && MathAbs(request.tp/size-MathRound(request.tp/size))>0.000001))
         { Error(h,"Native volume/tick bounds violated"); return; }
      if((request.type==ORDER_TYPE_BUY && request.sl>=tick.bid) ||
         (request.type==ORDER_TYPE_SELL && request.sl<=tick.ask)) { Error(h,"Stop-loss is not protective"); return; }
      if(tick.ask-tick.bid>StringToDouble(Get("max_spread",true)) ||
         AccountInfoDouble(ACCOUNT_EQUITY)<StringToDouble(Get("min_equity",true)))
         { Error(h,"Spread/equity risk bound violated"); return; }
      double risk=0,margin=0,price=request.type==ORDER_TYPE_BUY ? tick.ask : tick.bid;
      if(!OrderCalcProfit(request.type,symbol,request.volume,price,request.sl,risk) ||
         !OrderCalcMargin(request.type,symbol,request.volume,price,margin) ||
         risk>=0 || -risk+StringToDouble(Get("cost_buffer",true))>StringToDouble(Get("max_order_risk",true)) ||
         margin>StringToDouble(Get("max_order_margin",true))) { Error(h,"Native cash-risk/margin bound violated"); return; }
      Put(h,"estimated_stop_risk",Num(-risk)); Put(h,"estimated_margin",Num(margin));
   }
   request.price=request.type==ORDER_TYPE_BUY ? tick.ask : tick.bid;
   if(!OrderCheck(request,check) || check.retcode!=0) { Error(h,"Broker OrderCheck rejected"); Put(h,"check_retcode",IntegerToString(check.retcode)); Put(h,"check_comment",check.comment); return; }
   if(op=="check") { Put(h,"status","ok"); Put(h,"check_retcode",IntegerToString(check.retcode)); return; }
   // The processing file was claimed durably before this point. Never replay it after a crash.
   if(!Bound() || StringToInteger(Get("expires"))<(long)TimeGMT()) { Error(h,"Account/request expired before submit"); return; }
   if(!exit_only)
   {
      int sent=FileOpen(ROOT+"sent\\"+Get("magic")+".tsv",FILE_WRITE|FILE_CSV|FILE_ANSI,'\t',CP_UTF8);
      if(sent==INVALID_HANDLE) { Error(h,"Cannot persist order ownership"); return; }
      Put(sent,"request",id); Put(sent,"approval",digest); FileFlush(sent); FileClose(sent);
   }
   bool accepted=OrderSend(request,result);
   Put(h,"status","ok"); Put(h,"order_send_returned",accepted ? "1" : "0");
   Put(h,"retcode",IntegerToString(result.retcode)); Put(h,"order",IntegerToString((long)result.order));
   Put(h,"deal",IntegerToString((long)result.deal)); Put(h,"filled_volume",Num(result.volume));
   Put(h,"fill_price",Num(result.price)); Put(h,"broker_comment",result.comment);
}

void Read(int h,string op)
{
   string symbol=Get("symbol");
   if(op=="account")
   {
      Put(h,"connected",IntegerToString(TerminalInfoInteger(TERMINAL_CONNECTED)));
      Put(h,"currency",AccountInfoString(ACCOUNT_CURRENCY)); Put(h,"ea_sha256",EA_SHA256);
      Put(h,"balance",Num(AccountInfoDouble(ACCOUNT_BALANCE))); Put(h,"equity",Num(AccountInfoDouble(ACCOUNT_EQUITY)));
      Put(h,"free_margin",Num(AccountInfoDouble(ACCOUNT_MARGIN_FREE))); Put(h,"margin",Num(AccountInfoDouble(ACCOUNT_MARGIN)));
      Put(h,"leverage",IntegerToString(AccountInfoInteger(ACCOUNT_LEVERAGE)));
      Put(h,"trading_input",EnableTrading ? "1" : "0"); Put(h,"allow_real_input",AllowRealAccount ? "1" : "0");
      Put(h,"terminal_trade_allowed",IntegerToString(TerminalInfoInteger(TERMINAL_TRADE_ALLOWED)));
      Put(h,"utc_epoch",IntegerToString((long)TimeGMT())); Put(h,"server_epoch",IntegerToString((long)TimeTradeServer()));
   }
   else if(op=="symbols")
   {
      int total=SymbolsTotal(false);
      if(total>5000) { Error(h,"Symbol catalog exceeds the fixed read budget"); return; }
      Put(h,"count",IntegerToString(total));
      for(int i=0;i<total;i++)
      {
         string name=SymbolName(i,false),p="s."+IntegerToString(i+1)+".";
         Put(h,p+"symbol",name); Put(h,p+"description",SymbolInfoString(name,SYMBOL_DESCRIPTION));
      }
   }
   else if(op=="positions" || op=="orders")
   {
      int total=op=="positions" ? PositionsTotal() : OrdersTotal();
      Put(h,"count",IntegerToString(total));
      if(total>1000) { Error(h,"Account snapshot too large"); return; }
      for(int i=0;i<total;i++)
      {
         string prefix="p."+IntegerToString(i+1)+".";
         ulong ticket=op=="positions" ? PositionGetTicket(i) : OrderGetTicket(i);
         Put(h,prefix+"ticket",IntegerToString((long)ticket));
         Put(h,prefix+"symbol",op=="positions" ? PositionGetString(POSITION_SYMBOL) : OrderGetString(ORDER_SYMBOL));
         Put(h,prefix+"magic",IntegerToString(op=="positions" ? PositionGetInteger(POSITION_MAGIC) : OrderGetInteger(ORDER_MAGIC)));
         Put(h,prefix+"volume",Num(op=="positions" ? PositionGetDouble(POSITION_VOLUME) : OrderGetDouble(ORDER_VOLUME_CURRENT)));
         Put(h,prefix+"type",IntegerToString(op=="positions" ? PositionGetInteger(POSITION_TYPE) : OrderGetInteger(ORDER_TYPE)));
         if(op=="positions") { Put(h,prefix+"profit",Num(PositionGetDouble(POSITION_PROFIT))); Put(h,prefix+"sl",Num(PositionGetDouble(POSITION_SL))); }
      }
   }
   else if(op=="deals")
   {
      if(!Numeric(Get("start"),true) || !Numeric(Get("end"),true) || StringToInteger(Get("end"))<StringToInteger(Get("start")) ||
         !HistorySelect((datetime)StringToInteger(Get("start")),(datetime)StringToInteger(Get("end"))))
         { Error(h,"Invalid server-time deal window"); return; }
      int total=HistoryDealsTotal();
      if(total>10000) { Error(h,"Deal window exceeds budget; request shorter windows"); return; }
      Put(h,"count",IntegerToString(total));
      for(int i=0;i<total;i++)
      {
         ulong ticket=HistoryDealGetTicket(i); string prefix="d."+IntegerToString(i+1)+".";
         Put(h,prefix+"ticket",IntegerToString((long)ticket)); Put(h,prefix+"magic",IntegerToString(HistoryDealGetInteger(ticket,DEAL_MAGIC)));
         Put(h,prefix+"order",IntegerToString(HistoryDealGetInteger(ticket,DEAL_ORDER)));
         Put(h,prefix+"position",IntegerToString(HistoryDealGetInteger(ticket,DEAL_POSITION_ID)));
         Put(h,prefix+"symbol",HistoryDealGetString(ticket,DEAL_SYMBOL)); Put(h,prefix+"server_time",IntegerToString(HistoryDealGetInteger(ticket,DEAL_TIME)));
         Put(h,prefix+"volume",Num(HistoryDealGetDouble(ticket,DEAL_VOLUME))); Put(h,prefix+"price",Num(HistoryDealGetDouble(ticket,DEAL_PRICE)));
         Put(h,prefix+"profit",Num(HistoryDealGetDouble(ticket,DEAL_PROFIT))); Put(h,prefix+"commission",Num(HistoryDealGetDouble(ticket,DEAL_COMMISSION)));
         Put(h,prefix+"swap",Num(HistoryDealGetDouble(ticket,DEAL_SWAP))); Put(h,prefix+"fee",Num(HistoryDealGetDouble(ticket,DEAL_FEE)));
      }
   }
   else if(op=="symbol" || op=="quote" || op=="rates")
   {
      if(!SymbolReady(symbol)) { Error(h,"Unknown/unavailable symbol"); return; }
      if(op=="symbol")
      {
         Put(h,"symbol",symbol); Put(h,"description",SymbolInfoString(symbol,SYMBOL_DESCRIPTION));
         Put(h,"digits",IntegerToString(SymbolInfoInteger(symbol,SYMBOL_DIGITS)));
         Put(h,"tick_size",Num(SymbolInfoDouble(symbol,SYMBOL_TRADE_TICK_SIZE))); Put(h,"tick_value",Num(SymbolInfoDouble(symbol,SYMBOL_TRADE_TICK_VALUE)));
         Put(h,"contract_size",Num(SymbolInfoDouble(symbol,SYMBOL_TRADE_CONTRACT_SIZE))); Put(h,"volume_min",Num(SymbolInfoDouble(symbol,SYMBOL_VOLUME_MIN)));
         Put(h,"volume_max",Num(SymbolInfoDouble(symbol,SYMBOL_VOLUME_MAX))); Put(h,"volume_step",Num(SymbolInfoDouble(symbol,SYMBOL_VOLUME_STEP)));
         Put(h,"profit_currency",SymbolInfoString(symbol,SYMBOL_CURRENCY_PROFIT)); Put(h,"margin_currency",SymbolInfoString(symbol,SYMBOL_CURRENCY_MARGIN));
         Put(h,"swap_long",Num(SymbolInfoDouble(symbol,SYMBOL_SWAP_LONG))); Put(h,"swap_short",Num(SymbolInfoDouble(symbol,SYMBOL_SWAP_SHORT)));
         Put(h,"swap_mode",IntegerToString(SymbolInfoInteger(symbol,SYMBOL_SWAP_MODE)));
         Put(h,"swap_triple_day",IntegerToString(SymbolInfoInteger(symbol,SYMBOL_SWAP_ROLLOVER3DAYS)));
         Put(h,"trade_mode",IntegerToString(SymbolInfoInteger(symbol,SYMBOL_TRADE_MODE)));
      }
      if(op=="quote")
      {
         MqlTick tick;
         if(!SymbolInfoTick(symbol,tick)) { Error(h,"Quote unavailable"); return; }
         Put(h,"bid",Num(tick.bid)); Put(h,"ask",Num(tick.ask)); Put(h,"server_time_ms",IntegerToString(tick.time_msc));
         Put(h,"age_ms",IntegerToString((long)TimeTradeServer()*1000-tick.time_msc));
      }
      if(op=="rates")
      {
         string frame=Get("frame"); ENUM_TIMEFRAMES tf=PERIOD_CURRENT;
         ENUM_TIMEFRAMES frames[]={PERIOD_M1,PERIOD_M2,PERIOD_M3,PERIOD_M4,PERIOD_M5,PERIOD_M6,PERIOD_M10,PERIOD_M12,PERIOD_M15,PERIOD_M20,PERIOD_M30,
            PERIOD_H1,PERIOD_H2,PERIOD_H3,PERIOD_H4,PERIOD_H6,PERIOD_H8,PERIOD_H12,PERIOD_D1,PERIOD_W1,PERIOD_MN1};
         for(int i=0;i<ArraySize(frames);i++) if(EnumToString(frames[i])==frame) tf=frames[i];
         int count=(int)StringToInteger(Get("count"));
         if(tf==PERIOD_CURRENT || !Numeric(Get("count"),true) || count<1 || count>100000 || count>TerminalInfoInteger(TERMINAL_MAXBARS))
            { Error(h,"Invalid/budget-exceeding interval or count"); return; }
         MqlRates rates[]; ResetLastError();
         int copied;
         if(Get("end")!="") copied=CopyRates(symbol,tf,(datetime)StringToInteger(Get("end")),count,rates);
         else copied=CopyRates(symbol,tf,1,count,rates); // latest closed bars, not the forming bar
         if(copied<=0) { Error(h,"History unavailable; no silent retry"); Put(h,"native_error",IntegerToString(GetLastError())); return; }
         Put(h,"count",IntegerToString(copied)); Put(h,"time_domain","MT5 server/chart; UTC mapping unverified");
         for(int i=0;i<copied;i++)
         {
            string p="b."+IntegerToString(i+1)+".";
            Put(h,p+"time",IntegerToString((long)rates[i].time)); Put(h,p+"open",Num(rates[i].open));
            Put(h,p+"high",Num(rates[i].high)); Put(h,p+"low",Num(rates[i].low)); Put(h,p+"close",Num(rates[i].close));
            Put(h,p+"tick_volume",IntegerToString(rates[i].tick_volume)); Put(h,p+"spread",IntegerToString(rates[i].spread));
         }
      }
   }
   else { Error(h,"Unsupported typed operation"); return; }
   Put(h,"status","ok");
}

void Process(string filename)
{
   string id=StringSubstr(filename,0,32);
   if(!Hex(id,32) || filename!=id+".tsv") return;
   string claim=ROOT+"processing\\"+filename;
   if(FileIsExist(claim) || !FileMove(ROOT+"requests\\"+filename,0,claim,0)) return;
   bool loaded=Load(claim);
   string op=loaded ? Get("op") : "invalid";
   int h=FileOpen(ROOT+"responses\\"+id+".tmp",FILE_WRITE|FILE_CSV|FILE_ANSI,'\t',CP_UTF8);
   if(h==INVALID_HANDLE) return;
   Header(h,id,op);
   bool connected_before=(bool)TerminalInfoInteger(TERMINAL_CONNECTED);
   if(!loaded || Get("schema")!=SCHEMA || Get("id")!=id || Get("session")!=session ||
      Get("login")!=IntegerToString(bound_login) || Get("server")!=bound_server || Get("mode")!=bound_mode || !Bound() ||
      !Numeric(Get("expires"),true) || StringToInteger(Get("expires"))<(long)TimeGMT()) Error(h,"Expired, malformed or mismatched request/account/session");
   else if(!connected_before && op!="account") Error(h,"Terminal disconnected; cached data is not a current feed");
   else if(op=="check" || op=="submit" || op=="close") Trade(h,op,id);
   else Read(h,op);
   if(!Bound() || (connected_before && !TerminalInfoInteger(TERMINAL_CONNECTED) && op!="account"))
      { FileClose(h); Print("stratosphere: account/connection changed during request; response not published"); return; }
   FileFlush(h); FileClose(h);
   FileMove(ROOT+"responses\\"+id+".tmp",0,ROOT+"responses\\"+filename,0);
}
void OnTimer()
{
   if(!Bound()) { EventKillTimer(); Print("stratosphere: account changed; reattach EA explicitly"); return; }
   string name;
   long search=FileFindFirst(ROOT+"requests\\*.tsv",name);
   if(search==INVALID_HANDLE) return;
   Process(name); FileFindClose(search);
}

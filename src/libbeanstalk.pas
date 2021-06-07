unit libbeanstalk;

{$mode objfpc}{$H+}

{$DEFINE DEBUG}

interface

uses
  Classes,
  blcksock;

const
  CBeanstalk_MajorVersion = 1;
  CBeanstalk_MinorVersion = 3;
  CBeanstalk_PatchVersion = 0;

  CBeanstalk_MessageNoBody = 0;
  CBeanstalk_MessageHasBody = 1;

  CBeanstalkResponse_Using        = 'USING';
  CBeanstalkResponse_Watching     = 'WATCHING';
  CBeanstalkResponse_Inserted     = 'INSERTED';
  CBeanstalkResponse_Buried       = 'BURIED';
  CBeanstalkResponse_ExpectedCrLf = 'EXPECTED_CRLF';
  CBeanstalkResponse_JobTooBig    = 'JOB_TOO_BIG';
  CBeanstalkResponse_Draining     = 'DAINING';
  CBeanstalkResponse_Reserved     = 'RESERVED';
  CBeanstalkResponse_DeadlineSoon = 'DEADLINE_SOON';
  CBeanstalkResponse_TimedOut     = 'TIMED_OUT';
  CBeanstalkResponse_Deleted      = 'DELETED';
  CBeanstalkResponse_NotFound     = 'NOT_FOUND';
  CBeanstalkResponse_Released     = 'RELEASED';
  CBeanstalkResponse_Touched      = 'TOUCHED';
  CBeanstalkResponse_NotIgnored   = 'NOT_IGNORED';
  CBeanstalkResponse_Found        = 'FOUND';
  CBeanstalkResponse_Kicked       = 'KICKED';
  CBeanstalkResponse_Ok           = 'OK';

  CBeanstalkResponse_OutOfMemory    = 'OUT_OF_MEMORY';
  CBeanstalkResponse_InternalError  = 'INTERNAL_ERROR';
  CBeanstalkResponse_BadFormat      = 'BAD_FORMAT';
  CBeanstalkResponse_UnknownCommand = 'UNKNOWN_COMMAND';

type

  EBeanstalkStatus = (
              EBeanstalkStatus_Ok,
              EBeanstalkStatus_Fail,
              EBeanstalkStatus_ExpectedCrLf,
              EBeanstalkStatus_JobTooBig,
              EBeanstalkStatus_Draining,
              EBeanstalkStatus_TimedOut,
              EBeanstalkStatus_NotFound,
              EBeanstalkStatus_DeadlineSoon,
              EBeanstalkStatus_Ready,
              EBeanstalkStatus_Reserved,
              EBeanstalkStatus_Delayed,
              EBeanstalkStatus_Buried,
              EBeanstalkStatus_NotIgnored,
              EBeanstalkStatus_OutOfMemory,
              EBeanstalkStatus_InternalError,
              EBeanstalkStatus_BadFormat,
              EBeanstalkStatus_UnknownCommand,
              EBeanstalkStatus_Kicked);

  TBeanstalkBytes = Array of Byte;
  PBeanstalkBytes = ^TBeanstalkBytes;

  TBeanstalkJob = class(TObject)
    private
      FId : LongInt;
      FData : TBeanstalkBytes;

    public
      constructor Create(id : Integer; data : TBeanstalkBytes);
      destructor Destroy(); override;

      property Id : LongInt read FId;
      property Data : TBeanstalkBytes read FData;
  end;

  TBeanstalkClient = class(TObject)
    private
      FSock : TTCPBlockSocket;
    protected

    public
      constructor Create();
      destructor Destroy(); override;

      function Connect(ip, port : AnsiString) : Boolean;
      (* procedure Disconnect(); *)

      (* Producer Methods *)
      function Use(tube : AnsiString) : EBeanstalkStatus;
      function PutBytes(bytesData : TBeanstalkBytes; var id : Integer; priority : Integer = 100; delay : Integer = 0; ttr : Integer = 600) : EBeanstalkStatus;
      function PutString(stringData : AnsiString; var id : Integer; priority : Integer = 100; delay : Integer = 0; ttr : Integer = 600) : EBeanstalkStatus;

      (* Consumer Methods *)
      function Watch(tube : AnsiString) : EBeanstalkStatus;
      function Ignore(tube : AnsiString) : EBeanstalkStatus;

      function Reserve(): TBeanstalkJob;
      function ReserveTimeout(seconds : Integer) : TBeanstalkJob;

      function Delete(job : TBeanstalkJob): EBeanstalkStatus;

  end;

const
  BeanstalkStatusNames : Array[EBeanstalkStatus] of AnsiString = (
              'OK',
              'Fail',
              'Expected CRLF',
              'Job Too Big',
              'Draining',
              'Timed Out',
              'Not Found',
              'Deadline Soon',
              'Ready',
              'Reserved',
              'Delayed',
              'Buried',
              'Not Ignored',
              'Server Out Of Memory',
              'Internal Error',
              'Bad Format',
              'Unknown Command',
              'Kicked');

implementation

uses SysUtils, StrUtils;


function BeanstalkIsErrorResponse(res : AnsiString) : Boolean;
begin
  if AnsiStartsStr(CBeanstalkResponse_ExpectedCrLf, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_JobTooBig, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_TimedOut, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_NotFound, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_OutOfMemory, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_InternalError, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_BadFormat, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_UnknownCommand, res) then
    Exit(True)
  else if AnsiStartsStr(CBeanstalkResponse_Kicked, res) then
    Exit(True);

  Result := False;
end;

function BeanstalkGetErrorResponse(res : AnsiString) : EBeanstalkStatus;
begin
  if AnsiStartsStr(CBeanstalkResponse_ExpectedCrLf, res) then
    Exit(EBeanstalkStatus_ExpectedCrLf)
  else if AnsiStartsStr(CBeanstalkResponse_JobTooBig, res) then
    Exit(EBeanstalkStatus_JobTooBig)
  else if AnsiStartsStr(CBeanstalkResponse_TimedOut, res) then
    Exit(EBeanstalkStatus_TimedOut)
  else if AnsiStartsStr(CBeanstalkResponse_NotFound, res) then
    Exit(EBeanstalkStatus_NotFound)
  else if AnsiStartsStr(CBeanstalkResponse_OutOfMemory, res) then
    Exit(EBeanstalkStatus_OutOfMemory)
  else if AnsiStartsStr(CBeanstalkResponse_InternalError, res) then
    Exit(EBeanstalkStatus_InternalError)
  else if AnsiStartsStr(CBeanstalkResponse_BadFormat, res) then
    Exit(EBeanstalkStatus_BadFormat)
  else if AnsiStartsStr(CBeanstalkResponse_UnknownCommand, res) then
    Exit(EBeanstalkStatus_UnknownCommand)
  else if AnsiStartsStr(CBeanstalkResponse_Kicked, res) then
    Exit(EBeanstalkStatus_Kicked);

  Result := EBeanstalkStatus_InternalError;
end;

function BeanstalkGetStatusName(status : EBeanstalkStatus) : AnsiString;
begin
  Result := BeanstalkStatusNames[status];
end;

constructor TBeanstalkJob.Create(id : Integer; data : TBeanstalkBytes);
var
  copied : Integer;
  i : Integer;

begin
  self.FId := id;

  {$IFDEF DEBUG }
  WriteLn('data.Size: ', Length(data));
  Write('Loaded: ');
  for i := 0 to Length(data) - 1 do
    Write(Char(data[i]));
  WriteLn();
  {$ENDIF}

  (* Allocate Byte Array to hold data *)
  SetLength(self.FData, Length(data) - 1);
  
  WriteLn('SetLength!');

  (* Copy the data from TMemoryStream (data) into Byte Array *)
  Move(data[0], FData[0], Length(data) - 1);
  WriteLn('Move!');

  (* Resize buffer to fit data - For some reason Move increases size to 4K *)
  //SetLength(FData, Length(data) - 1);
  //WriteLn('Re-SetLength!');
  
  FData[Length(data) - 2] := 0;
  WriteLn('Set Null Terminator!');

  {$IFDEF DEBUG}
  WriteLn('Size of ByteArray: ', Length(FData));
  WriteLn('Data: ', PChar(FData));
  {$ENDIF}
  //self.FData[Length(self.FData)-1] := Byte(#0);

  //WriteLn('Data: ', PChar(self.FData))
end;

destructor TBeanstalkJob.Destroy();
begin
  if Assigned(self.FData) then
  begin
    SetLength(self.FData, 0);
    self.FData := nil;
  end;
  

  inherited Destroy();
end;

constructor TBeanstalkClient.Create();
begin
  self.FSock := TTCPBlockSocket.Create();
end;

destructor TBeanstalkClient.Destroy();
begin
  WriteLn('Freeing TBeanstalkClient.');
  FreeAndNil(self.FSock);

  inherited Destroy();
end;


function TBeanstalkClient.Connect(ip, port : AnsiString) : Boolean;
begin
  Result := True;
  self.FSock.Connect(ip, port);

  if self.FSock.LastError <> 0 then
  begin
    {$IFDEF DEBUG}
    WriteLn('Could not connect to server.');
    {$ENDIF}

    exit (False);
  end;

  (* TODO: There is still a lot more to do here *)
end;

function TBeanstalkClient.Use(tube : AnsiString): EBeanstalkStatus;
var
  res : AnsiString;
begin
  Result := EBeanstalkStatus_Ok;

  self.FSock.SendString('use ' + tube + #13#10);
  res := self.FSock.RecvString(5000);

  {$IFDEF DEBUG}
  WriteLn(res);
  {$ENDIF}

  (* Check result is succes *)
  if AnsiStartsStr(CBeanstalkResponse_Using, res) then
    Exit(EBeanstalkStatus_Ok);

  (* Check for Error responses *)
  if BeanstalkIsErrorResponse(res) then
  begin
    Result := BeanstalkGetErrorResponse(res);
  end;
end;

function TBeanstalkClient.PutBytes(bytesData : TBeanstalkBytes; var id : Integer; priority : Integer = 100; delay : Integer = 0; ttr : Integer = 600) : EBeanstalkStatus;
var
  msg : AnsiString;
  res : AnsiString;
  str : AnsiString = '';
  i : Integer = 0;
  stream : TMemoryStream = nil;
  bytesTmp : TBeanstalkBytes;
begin
  WriteLn('TBeanstalkClient.PutBytes');

  WriteLn('Data Sending: ');
  //WriteLn(bytesData);
  for i := 0 to Length(bytesData) - 1 do
  begin
    Write(IntToStr(bytesData[i]) + ' ');
  end;
  WriteLn();

  (* Build put command string *)
  msg := 'put ' + IntToStr(priority) + ' ' + IntToStr(delay) + ' ' + IntToStr(ttr) + ' ' + IntToStr(Length(bytesData)) + #13#10;

  WriteLn('Length of msg: ' + IntToStr(Length(msg)));
  WriteLn(msg);

  (* Populate the Byte Data TMemoryStream to send to the server *)
  stream := TMemoryStream.Create();
  stream.SetSize(Length(msg) + Length(bytesData) + 2);
  stream.WriteBuffer(PChar(msg)^, Length(msg)); // Write the String Bytes (This is UTF-8 Safe)
  WriteLn('msg written: POS -> ', IntToStr(stream.Position));
  stream.WriteBuffer(bytesData[0], Length(bytesData)); // Write the ByteArray from position 0 for length
  WriteLn('bytesData written: POS -> ', IntToStr(stream.Position));
  stream.WriteByte(Byte(13));
  stream.WriteByte(Byte(10));
  WriteLn('StreamSize: ', IntToStr(stream.Size));
  SetLength(bytesTmp, stream.Size);
  stream.Position := 0;
  stream.ReadBuffer(bytesTmp[0], stream.Size);



  WriteLn(#13#10#13#10 + 'Memory Stream Data #: ');
  for i := 0 to stream.Size - 1 do
  begin
    Write(IntToStr(bytesTmp[i]) + ' ');
  end;
  WriteLn();



  (* Send to server *)
  self.FSock.SendBuffer(@bytesTmp[0], Length(bytesTmp));

  (* Wait for server to respond *)
  while res = '' do
  begin
    res := self.FSock.RecvString(5000);
    WriteLn(res);
  end;

  (* Free Memory *)
  FreeAndNil(stream);
  SetLength(bytesTmp, 0);

  (* Check result is success *)
  if AnsiStartsStr(CBeanstalkResponse_Inserted, res) then
  begin
    (* Set the id inserted *)
    sscanf(res, '%s %d', [@str, @id]);


    Exit(EBeanstalkStatus_Ok);
  end;

  (* Check for Error Responses *)
  if BeanstalkIsErrorResponse(res) then
  begin
    Result := BeanstalkGetErrorResponse(res);
  end;

end;

function TBeanstalkClient.PutString(stringData : AnsiString; var id : Integer; priority : Integer = 100; delay : Integer = 0; ttr : Integer = 600) : EBeanstalkStatus;
var
  msg : AnsiString;
  res : AnsiString = '';
  str : AnsiString = '';
begin
  {$IFDEF DEBUG}
  WriteLn('TBeanstalkClient.PutString');

  WriteLn('Data Sending: ');
  WriteLn(stringData);

  WriteLn('Length: ', Length(stringData), ' IntToStr: ', IntToStr(Length(stringData)));
  {$ENDIF}

  (* Assemble Message *)
  msg := 'put ' + IntToStr(priority) + ' ' + IntToStr(delay) + ' ' + IntToStr(ttr) + ' ' + IntToStr(Length(stringData)) + #13#10;
  msg := msg + stringData + #13#10;


  self.FSock.SendString(msg);
  while res = '' do
  begin
    res := self.FSock.RecvString(5000);
  end;

  {$IFDEF DEBUG}
  WriteLn(res);
  {$ENDIF}

  (* Check result is success *)
  if AnsiStartsStr(CBeanstalkResponse_Inserted, res) then
  begin
    (* Set the id inserted *)
    sscanf(res, '%s %d', [@str, @id]);
    Exit(EBeanstalkStatus_Ok);
  end;

  (* Check for Error Responses *)
  if BeanstalkIsErrorResponse(res) then
  begin
    Result := BeanstalkGetErrorResponse(res);
  end;
end;

function TBeanstalkClient.Watch(tube : AnsiString) : EBeanstalkStatus;
var
  msg : AnsiString;
  res : AnsiString;
begin
  msg := 'watch ' + tube + #13#10;

  self.FSock.SendString(msg);
  res := self.FSock.RecvString(5000);

  (* Check for valid response *)
  if AnsiStartsStr(CBeanstalkResponse_Watching, res) then
  begin
    Exit(EBeanstalkStatus_Ok);
  end;

  (* Check for Error Responses *)
  if BeanstalkIsErrorResponse(res) then
  begin
    Result := BeanstalkGetErrorResponse(res);
  end;
end;

function TBeanstalkClient.Ignore(tube : AnsiString) : EBeanstalkStatus;
var
  msg : AnsiString;
  res : AnsiString;
begin
  msg := 'ignore ' + tube + #13#10;

  self.FSock.SendString(msg);
  res := self.FSock.RecvString(1000);

  (* Check for valid response *)
  if AnsiStartsStr(CBeanstalkResponse_Watching, res) then
  begin
    Exit(EBeanstalkStatus_Ok);
  end;

  (* Check for Error Responses *)
  if BeanstalkIsErrorResponse(res) then
  begin
    Result := BeanstalkGetErrorResponse(res);
  end;
end;

function TBeanstalkClient.Reserve(): TBeanstalkJob;
var
  msg : AnsiString;
  res : AnsiString;
  id : Integer;
  bodySize : Integer;
  readBuffer : TMemoryStream = nil;
  x, i : Integer;
  jobDataBuffer : TBeanstalkBytes;
begin


  msg := 'reserve' + #13#10;
  self.FSock.SendString(msg);

  while res = '' do
  begin
    res := self.FSock.RecvString(1000);
    WriteLn('Nothing reserved, request again');
  end;

  {$IFDEF DEBUG}
  WriteLn(res);
  {$ENDIF}

  (* Parse received data *)
  if AnsiStartsStr(CBeanstalkResponse_Reserved, res) then
  begin
    sscanf(res, 'RESERVED %d %d', [@id, @bodySize]);

    WriteLn('JobID: ', id);
    WriteLn('JobSize: ', bodySize);

    SetLength(jobDataBuffer, bodySize + 2); // Add 2 Bytes for trailing CRLF
    WriteLn('Expect BodySize: ' + IntToStr(Length(jobDataBuffer)));


    (* Receive the body into buffer *)
    x := self.FSock.RecvBufferEx(PChar(jobDataBuffer), Length(jobDataBuffer), 2000);
    
    (* Print bytes as string? *)
    //WriteLn('Received Bytes: ', AnsiString(PChar(readBuffer.Memory)));
    WriteLn('Received Bytes: ');
    for i := 0 to Length(jobDataBuffer) - 1 do
    begin
      Write(Char(jobDataBuffer[i]));
    end;
    WriteLn();

    (* Create BeanstalkJob to return *)
    //Result := TBeanstalkJob.Create(id, readBuffer);
    //Result := nil;
    Result := TBeanstalkJob.Create(id, jobDataBuffer);
    //id : Integer; data : TMemoryStream
    //Result.FId := id;
    //SetLength(Result.FData, bodySize);
    //Result.FData := Copy(jobDataBuffer, 1, bodySize);
    //Move(jobDataBuffer[0], Result.FData, bodySize);
    WriteLn('MOVED!!!');
    
    (* Free memory *)
    if Assigned(readBuffer) then FreeAndNil(readBuffer);
    SetLength(jobDataBuffer, 0);

    exit;
  end
  else
  begin
    (* We received something other than a job *)

    (* Check for Error Responses *)
    if BeanstalkIsErrorResponse(res) then
    begin
      WriteLn('Error:');
    end;

    WriteLn(res);
    exit(nil);
  end;


  Result := nil;
end;

function TBeanstalkClient.ReserveTimeout(seconds : Integer) : TBeanstalkJob;
begin

end;

function TBeanstalkClient.Delete(job : TBeanstalkJob): EBeanstalkStatus;
var
  msg : AnsiString;
  res : AnsiString = '';
  id : Integer;
begin
  (* Get the ID from the Job *)
  id := job.Id;

  msg := 'delete ' + IntToStr(id) + #13#10;
  self.FSock.SendString(msg);

  (* Get confirmation or response *)
  while res = '' do
  begin
    res := self.FSock.RecvString(1000);
  end;

  {$IFDEF DEBUG}
  WriteLn(res);
  {$ENDIF}

  (* Check for valid/success response *)
  if AnsiStartsStr(CBeanstalkResponse_Deleted, res) then
  begin
    Result := EBeanstalkStatus_Ok;
    exit;
  end;

  (* Check for Error Responses *)
  if BeanstalkIsErrorResponse(res) then
  begin
    Result := BeanstalkGetErrorResponse(res);
  end;
end;


end.

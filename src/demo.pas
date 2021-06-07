program demo;

{$mode objfpc}{$H+}

uses SysUtils, libbeanstalk;

procedure main();
var
  beanstalk : TBeanstalkClient;
  insert_id : Integer;
  bytes : TBeanstalkBytes;
  i : Integer = 0;
begin
  beanstalk := TBeanstalkClient.Create();

  beanstalk.Connect('127.0.0.1', '11300');

  beanstalk.Use('sometube');

  //beanstalk.PutString('This is a super test.', insert_id);

  // Test Bytes
  SetLength(bytes, 10);
  for i := 0 to 9 do
  begin
    // Load array
    bytes[i] := i + 65;
  end;
  beanstalk.PutBytes(bytes, insert_id);

  WriteLn('Insert ID: ', insert_id);

  SetLength(bytes, 0);

  FreeAndNil(beanstalk);

end;

begin

  main();

end.

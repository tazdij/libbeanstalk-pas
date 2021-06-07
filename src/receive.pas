program demo;

uses libbeanstalk, libfcgi, SysUtils;

procedure main();
var
  beanstalk : TBeanstalkClient;
  job : TBeanstalkJob;
begin
  beanstalk := TBeanstalkClient.Create();

  beanstalk.Connect('127.0.0.1', '11300');

  beanstalk.Watch('sometube');

  (* Reserve should loop waiting for a job *)
  while True do
  begin
    job := beanstalk.Reserve();

    if Assigned(job) then
    begin
      WriteLn('Job Id: ', job.Id);
      WriteLn('Received: ', Length(job.Data));
      WriteLn('Received: ', PChar(job.Data));
      WriteLn('StringSize: ', StrLen(PChar(job.Data)));

      (* delete job from queue, not the local job object *)
      beanstalk.Delete(job);
      FreeAndNil(job);
    end
    else
        break;
  end;

end;

begin

  main();

end.

use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, Error};
use std::thread;

macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

///
/// Starts a new echo server that listens for connections in a background thread
/// responds to connections one at a time
/// function returns once echo server has been started and is listening
///
pub fn start_echo_server (port: u16) -> Result<(), Error>{
    let listener = try!(TcpListener::bind(("localhost", port)));
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => {
                    let _ = do_echo(&mut stream);
                }
                Err(e) => {
                    println_stderr!("{}", e);
                } 
            }
        }
    });
    Ok(())
}

fn do_echo(stream: &mut TcpStream) -> Result<(), Error>{
    let mut buf: String = String::new();
    stream.read_to_string(&mut buf)
          .and_then(|_| {
            stream.write_all(buf.as_bytes())
          })
}

#[cfg(test)]
mod tests {
    use super::start_echo_server;
    use std::net::{TcpStream, Shutdown};
    use std::io::{Read, Write};
    use std::time::Duration;

    #[test]
    fn it_echos() {
        // constants
        let port = 8080;
        let message = "Hello from\n test land!";

        // start the echo server and connect to it
        let _ = start_echo_server(port).unwrap();
        let mut client = TcpStream::connect(("localhost", port)).unwrap();

        // write to the server, send a fin, and wait for the response
        let mut result = String::new();
        let num_bytes_read = client.write_all(message.as_bytes())
        .and_then(|()| client.shutdown(Shutdown::Write))
        .and_then(|()| {
            client.read_to_string(&mut result)
        }).unwrap();

        // check that the response is correct
        assert!(num_bytes_read == message.len());
        assert!(result == message);
    }
}

use std::{net::SocketAddr, sync::{Arc, Mutex}};

use pyo3::{IntoPyObjectExt, exceptions::{PyConnectionError, PyStopIteration}, prelude::*};
use socket2::{Domain, Protocol, Socket, Type};

pub type OptionArc<T> = Option<Arc<Mutex<T>>>;

#[pyclass]
pub struct ConnectIo {
    pub addr: SocketAddr,
    pub yielded: bool
}

#[pymethods]
impl ConnectIo {
    #[new]
    fn new(addr: &str) -> PyResult<Self> {
        let parsed = addr.parse::<SocketAddr>()?;
        Ok(Self { addr: parsed, yielded: false })
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>>{
        if !slf.yielded {
            slf.yielded = true;
            let py = slf.py();
            return Ok(Some(slf.into_py_any(py).unwrap()));
        }
        Err(PyStopIteration::new_err(slf.py().None()))
    }
}

#[pyclass(unsendable)]
pub struct AcceptIo {
    pub listener_arc: OptionArc<mio::net::TcpListener>,
    pub yielded: bool
}

impl From<Arc<Mutex<mio::net::TcpListener>>> for AcceptIo {
    fn from(value: Arc<Mutex<mio::net::TcpListener>>) -> Self {
        AcceptIo {
            listener_arc: Some(value),
            yielded: false
        }
    }
}

#[pymethods]
impl AcceptIo {

    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(AcceptIo {
            listener_arc: None,
            yielded: false
        })
    }

    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>>{
        if !slf.yielded {
            slf.yielded = true;
            let py = slf.py();
            return Ok(Some(slf.into_py_any(py).unwrap()));
        }
        Err(PyStopIteration::new_err(slf.py().None()))
    }
}


#[pyclass(unsendable)]
pub struct ReadIo {
    pub listener_arc: OptionArc<mio::net::TcpStream>,
    pub size: usize,
    pub yielded: bool
}

#[pymethods]
impl ReadIo {

    #[new]
    pub fn new(size: usize) -> PyResult<Self> {
        Ok(ReadIo {
            listener_arc: None,
            size,
            yielded: false
        })
    }

    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>>{
        if !slf.yielded {
            slf.yielded = true;
            let py = slf.py();
            return Ok(Some(slf.into_py_any(py).unwrap()));
        }
        Err(PyStopIteration::new_err(slf.py().None()))
    }
}

#[pyclass(unsendable)]
pub struct WriteIo {
    pub listener_arc: OptionArc<mio::net::TcpStream>,
    pub data: Vec<u8>,
    pub yielded: bool
}

#[pymethods]
impl WriteIo {

    #[new]
    pub fn new(data: Vec<u8>) -> PyResult<Self> {
        Ok(WriteIo {
            listener_arc: None,
            data,
            yielded: false
        })
    }

    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>>{
        if !slf.yielded {
            slf.yielded = true;
            let py = slf.py();
            return Ok(Some(slf.into_py_any(py).unwrap()));
        }
        Err(PyStopIteration::new_err(slf.py().None()))
    }
}



#[pyclass(unsendable)]
pub struct PyTcpListener {
    pub inner: OptionArc<mio::net::TcpListener>,
}

#[pymethods]
impl PyTcpListener {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(PyTcpListener { inner: None })
    }

    pub fn bind(&mut self, addr: &str) -> PyResult<()> {
        let parsed_addr = addr.parse::<SocketAddr>()?;
        let socket = Socket::new(Domain::for_address(parsed_addr), Type::STREAM, Some(Protocol::TCP))?;
        socket.set_nonblocking(true)?;
        socket.bind(&parsed_addr.into())?;
        socket.listen(1024)?;
        let mio_sock = mio::net::TcpListener::from_std(socket.into());
        self.inner = Some(Arc::new(Mutex::new(mio_sock)));
        Ok(())
    }

    pub fn accept(&self) -> PyResult<AcceptIo> {
        match &self.inner {
            Some(arc) => Ok(
                AcceptIo::from(arc.clone())
            ),
            None => {
                Err(PyConnectionError::new_err("Socket not yet bound"))
            }
        }
    }
}


#[pyclass]
pub struct PyTcpStream {
    pub stream_arc: OptionArc<mio::net::TcpStream>
}

#[pymethods]
impl PyTcpStream {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(PyTcpStream { stream_arc: None})
    }

    pub fn connect(&self, addr: &str) -> PyResult<ConnectIo> {
        let parsed = addr.parse::<SocketAddr>()?;
        Ok(ConnectIo {addr: parsed, yielded: false})
    }

    pub fn read(&self, size: usize) -> PyResult<ReadIo> {
        match &self.stream_arc {
            Some(arc) => {
                Ok(ReadIo {
                    listener_arc: Some(arc.clone()),
                    size,
                    yielded: false
                })
            },
            None => {
                Err(PyConnectionError::new_err("Socket not yet connected"))
            }
        }
    }

    pub fn write(&self, data: Vec<u8>) -> PyResult<WriteIo> {
        match &self.stream_arc {
            Some(arc) => {
                Ok(WriteIo {
                    listener_arc: Some(arc.clone()),
                    data,
                    yielded: false
                })
            },
            None => {
                Err(PyConnectionError::new_err("Socket not yet connected"))
            }
        }
    }
}
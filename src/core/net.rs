use pyo3::{
    exceptions::{PyConnectionError, PyOSError, PyStopIteration},
    prelude::*,
    IntoPyObjectExt,
};
use std::net::SocketAddr;

macro_rules! impl_generator {
    ($name:ident, $field:ident, $type:ty) => {
        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
                if !slf.yielded {
                    slf.yielded = true;
                    let py = slf.py();
                    Ok(Some(slf.$field.clone_ref(py).into_py_any(py)?))
                } else {
                    Err(PyStopIteration::new_err(()))
                }
            }

            fn send(
                mut slf: PyRefMut<'_, Self>,
                value: Option<Py<PyAny>>,
            ) -> PyResult<Option<Py<PyAny>>> {
                let py = slf.py();
                if !slf.yielded {
                    slf.yielded = true;
                    Ok(Some(slf.$field.clone_ref(py).into_py_any(py)?))
                } else {
                    match value {
                        Some(val) => Err(PyStopIteration::new_err(val)),
                        None => Err(PyStopIteration::new_err(py.None())),
                    }
                }
            }

            fn throw(
                _slf: PyRefMut<'_, Self>,
                exc: Bound<'_, PyAny>,
            ) -> PyResult<Option<Py<PyAny>>> {
                Err(PyErr::from_value(exc))
            }
        }
    };
}

#[pyclass]
pub struct BindIo {
    pub addr: SocketAddr,
    pub pyclass: Py<PyTcpListener>,
}

impl BindIo {
    fn new(addr: SocketAddr, pylistener: Py<PyTcpListener>) -> Self {
        BindIo {
            addr,
            pyclass: pylistener,
        }
    }
}

#[pymethods]
impl BindIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<BindIoGen>> {
        let py = slf.py();
        let gen = BindIoGen {
            bind_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct BindIoGen {
    bind_io: Py<BindIo>,
    yielded: bool,
}

impl_generator!(BindIoGen, bind_io, BindIo);

#[pyclass]
pub struct ConnectIo {
    pub addr: SocketAddr,
    pub pyclass: Py<PyTcpStream>,
}

#[pymethods]
impl ConnectIo {
    #[new]
    pub fn new(addr: &str, py: Python) -> PyResult<Self> {
        let parsed = addr
            .parse::<SocketAddr>()
            .map_err(|e| PyOSError::new_err(format!("Invalid address: {}", e)))?;

        let py_stream = Py::new(py, PyTcpStream::empty())?;

        Ok(ConnectIo {
            addr: parsed,
            pyclass: py_stream,
        })
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<ConnectIoGen>> {
        let py = slf.py();
        let gen = ConnectIoGen {
            connect_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct ConnectIoGen {
    connect_io: Py<ConnectIo>,
    yielded: bool,
}

impl_generator!(ConnectIoGen, connect_io, ConnectIo);

#[pyclass(unsendable)]
pub struct AcceptIo {
    pub listener_id: usize,
}

impl AcceptIo {
    pub fn new(listener_id: usize) -> Self {
        AcceptIo { listener_id }
    }
}

#[pymethods]
impl AcceptIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<AcceptIoGen>> {
        let gen = AcceptIoGen {
            listener_id: slf.listener_id,
            yielded: false,
        };
        Py::new(slf.py(), gen)
    }
}

#[pyclass(unsendable)]
struct AcceptIoGen {
    listener_id: usize,
    yielded: bool,
}

#[pymethods]
impl AcceptIoGen {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        if !slf.yielded {
            slf.yielded = true;
            let py = slf.py();
            let op = AcceptIo::new(slf.listener_id);
            Ok(Some(op.into_py_any(py)?))
        } else {
            Err(PyStopIteration::new_err(()))
        }
    }

    fn send(mut slf: PyRefMut<'_, Self>, value: Option<Py<PyAny>>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        if !slf.yielded {
            slf.yielded = true;
            let op = AcceptIo::new(slf.listener_id);
            Ok(Some(op.into_py_any(py)?))
        } else {
            match value {
                Some(val) => Err(PyStopIteration::new_err(val)),
                None => Err(PyStopIteration::new_err(py.None())),
            }
        }
    }

    fn throw(_slf: PyRefMut<'_, Self>, exc: Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
        Err(PyErr::from_value(exc))
    }
}

#[pyclass(unsendable)]
pub struct ReadIo {
    pub size: usize,
    pub stream_id: usize,
}

impl ReadIo {
    pub fn new(size: usize, stream_id: usize) -> Self {
        ReadIo { size, stream_id }
    }
}

#[pymethods]
impl ReadIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<ReadIoGen>> {
        let py = slf.py();
        let gen = ReadIoGen {
            read_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct ReadIoGen {
    read_io: Py<ReadIo>,
    yielded: bool,
}

impl_generator!(ReadIoGen, read_io, ReadIo);

#[pyclass(unsendable)]
pub struct WriteIo {
    pub data: Vec<u8>,
    pub stream_id: usize,
}

impl WriteIo {
    pub fn new(data: Vec<u8>, stream_id: usize) -> Self {
        WriteIo { data, stream_id }
    }
}

#[pymethods]
impl WriteIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<WriteIoGen>> {
        let py = slf.py();
        let gen = WriteIoGen {
            write_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct WriteIoGen {
    write_io: Py<WriteIo>,
    yielded: bool,
}

impl_generator!(WriteIoGen, write_io, WriteIo);

#[pyclass(unsendable)]
pub struct PyTcpListener {
    pub addr: String,
    pub listener_id: Option<usize>,
}

impl PyTcpListener {
    pub fn set_listener_id(&mut self, id: usize) {
        self.listener_id = Some(id);
    }
}

#[pymethods]
impl PyTcpListener {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(PyTcpListener {
            addr: String::new(),
            listener_id: None,
        })
    }

    pub fn bind(slf: PyRef<'_, Self>, addr: &str) -> PyResult<BindIo> {
        let parsed = addr.parse::<SocketAddr>()?;
        Ok(BindIo::new(parsed, slf.into()))
    }

    pub fn accept(&self) -> PyResult<AcceptIo> {
        match self.listener_id {
            Some(id) => Ok(AcceptIo::new(id)),
            None => Err(PyConnectionError::new_err("Socket not yet bound")),
        }
    }
}

#[pyclass(unsendable)]
pub struct PyTcpStream {
    pub stream_id: Option<usize>,
    pub addr: Option<SocketAddr>,
    pub closed: bool,
}

impl PyTcpStream {
    pub fn empty() -> Self {
        PyTcpStream {
            stream_id: None,
            addr: None,
            closed: false,
        }
    }

    pub fn set_stream_id(&mut self, id: usize) {
        self.stream_id = Some(id);
    }

    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.addr = Some(addr);
    }
}

#[pymethods]
impl PyTcpStream {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(PyTcpStream::empty())
    }

    #[staticmethod]
    fn connect(addr: String, py: Python) -> PyResult<ConnectIo> {
        ConnectIo::new(&addr, py)
    }

    fn is_connected(&self) -> PyResult<bool> {
        Ok(self.addr.is_some())
    }

    fn peer_addr(&self) -> PyResult<String> {
        match self.addr {
            Some(addr) => Ok(addr.to_string()),
            None => Err(PyOSError::new_err("Not connected")),
        }
    }

    fn read(slf: PyRef<'_, Self>, size: usize) -> PyResult<ReadIo> {
        match slf.stream_id {
            Some(stream_id) => Ok(ReadIo::new(size, stream_id)),
            None => Err(PyOSError::new_err("Not connected")),
        }
    }

    fn write(slf: PyRef<'_, Self>, data: Vec<u8>) -> PyResult<WriteIo> {
        match slf.stream_id {
            Some(stream_id) => Ok(WriteIo::new(data, stream_id)),
            None => Err(PyOSError::new_err("Not connected")),
        }
    }
}

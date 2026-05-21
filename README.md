# ChunkedCopy

A lightweight, high-performance file copy utility that uses **chunked streaming** to efficiently copy large files with minimal memory overhead.

## 🚀 Overview

`ChunkedCopy` is designed to improve reliability and performance when copying large files by:

- Reading and writing data in **fixed-size chunks**
- Avoiding loading entire files into memory
- Providing better control over buffering behavior
- Supporting long-running or large file operations

This makes it especially useful for:
- Large media files (e.g., MKV, ISO)
- Network transfers
- Disk-to-disk operations
- Backup or migration tools

---

## ⚙️ Features

- ✅ Chunk-based file copying
- ✅ Low memory footprint
- ✅ Stream-based processing
- ✅ Simple and extensible design
- ✅ Suitable for large files

---

## 📦 Installation

Clone the repository:

```bash
git clone https://github.com/jhlassen17/ChunkedCopy.git
```

---

Open the project in Visual Studio or your preferred IDE.

## 🧑‍💻 Usage
Basic example:
```
c#

using System;
using System.IO;

class Program
{
    static void Main()
    {
        string source = @"C:\path\to\source.file";
        string destination = @"C:\path\to\destination.file";

        ChunkedCopy.Copy(source, destination);
    }
}
```

## 🔧 How It Works
Instead of copying a file in one operation, ChunkedCopy:

1. Opens a read stream for the source file
2. Opens a write stream for the destination file
3. Reads a fixed-size buffer (chunk)
4. Writes that chunk to the destination
5. Repeats until complete

Example Logic

```
byte[] buffer = new byte[81920]; // 80 KB

int bytesRead;
while ((bytesRead = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
{
    destinationStream.Write(buffer, 0, bytesRead);
}
```

---

## 📊 Why Chunked Copy?

| Method             | Memory Usage | Reliability | Large File Support |
|------------------|-------------|------------|--------------------|
| File.ReadAllBytes | High        | ❌         | ❌                 |
| File.Copy         | Medium      | ✅         | ✅                 |
| ChunkedCopy       | Low         | ✅✅        | ✅✅                |

---

## 🧪 Potential Enhancements

- Progress reporting
- Cancellation support
- Parallel chunk processing
- Retry logic for network copies
- Logging and diagnostics

---

## 🤝 Contributing
Contributions are welcome! Feel free to:

- Open issues
- Submit pull requests
- Suggest improvements

---

## 📄 License
This project is licensed under the MIT License.

See the full license in the LICENSE file.

---


## 👨‍💻 Author

**Jeffrey Lassen**  
Version: `1.2.1`  
Last Updated: `05/08/2026`

https://github.com/jhlassen17

---

## ☕ Support

If you find this useful:  
👉 https://buymeacoffee.com/hanf


---

## ⭐ Notes
This project is intentionally simple and focused. It can be used as:

- A utility
- A learning reference
- A building block for larger file-processing systems

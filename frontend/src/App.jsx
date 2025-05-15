import { useState } from "react";
import axios from "axios";

function App() {
  const [pdfFile, setPdfFile] = useState(null);
  const [downloadUrl, setDownloadUrl] = useState(null);

  const handleFileChange = (e) => {
    setPdfFile(e.target.files[0]);
    setDownloadUrl(null);
  };

  const handleUpload = async () => {
    if (!pdfFile) {
      alert("Selecciona un archivo PDF.");
      return;
    }

    const formData = new FormData();
    formData.append("pdf", pdfFile);

    try {
      const response = await axios.post("http://localhost:5000/upload", formData, {
        responseType: "blob",
      });

      const blob = new Blob([response.data], { type: "application/pdf" });
      const url = window.URL.createObjectURL(blob);
      setDownloadUrl(url);
    } catch (error) {
      console.error("Error:", error);
    }
  };

  return (
    <div style={{ padding: "2rem" }}>
      <h1>News Agent</h1>
      <input type="file" accept="application/pdf" onChange={handleFileChange} />
      <br /><br />
      <button onClick={handleUpload}>Subir PDF</button>
      {downloadUrl && (
        <a href={downloadUrl} download="archivo_descargado.pdf">
          Descargar PDF
        </a>
      )}
    </div>
  );
}

export default App;

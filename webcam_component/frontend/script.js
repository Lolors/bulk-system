const video = document.getElementById("video");
const captureButton = document.getElementById("capture");

// 카메라 스트림 시작
async function startCamera() {
  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      video: {
        width: { ideal: 1920 },
        height: { ideal: 1080 },
        facingMode: "environment"
      }
    });
    video.srcObject = stream;
  } catch (error) {
    console.error("Camera error:", error);
  }
}

startCamera();

// Streamlit → JS 통신용
const stFrame = window.parent;

// 촬영 버튼 클릭 시
captureButton.addEventListener("click", () => {
  const canvas = document.createElement("canvas");
  canvas.width = video.videoWidth;
  canvas.height = video.videoHeight;

  const ctx = canvas.getContext("2d");
  ctx.drawImage(video, 0, 0);

  // base64 PNG
  const dataUrl = canvas.toDataURL("image/png");

  // Python으로 전달
  stFrame.postMessage(
    {
      isStreamlitMessage: true,
      type: "webcamCapture",
      data: dataUrl,
    },
    "*"
  );
});


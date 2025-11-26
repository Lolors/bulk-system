import streamlit as st
import streamlit.components.v1 as components
import base64
import io
from PIL import Image

# Streamlit 컴포넌트 선언
_component_func = components.declare_component(
    "webcam_component",
    path="./webcam_component/frontend"
)

def webcam_component(key="webcam"):
    """고해상도 WebRTC 카메라 캡처 컴포넌트"""
    data = _component_func(key=key)
    if data:
        # data = base64 PNG
        header, encoded = data.split(",", 1)
        img_bytes = base64.b64decode(encoded)
        img = Image.open(io.BytesIO(img_bytes))
        return img
    return None


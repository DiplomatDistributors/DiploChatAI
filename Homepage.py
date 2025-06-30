import streamlit as st
from streamlit_elements import elements, mui
from st_bridge import bridge
import base64
from MainFunctions import * 

import requests
import urllib.parse
import os
import msal
from dotenv import load_dotenv

st.set_page_config(page_title="DiploChat", initial_sidebar_state="collapsed")
load_css()
load_dotenv()

# Azure AD
CLIENT_ID = "d37190f1-08a2-43f2-8434-9d1fa30ed7ae"
TENANT_ID = "258fd493-7c88-46ef-b497-f493b077448f"
CLIENT_SECRET = os.getenv("MICROSOFT_PROVIDER_AUTHENTICATION_SECRET")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
if os.getenv("RUNNING_IN_PRODUCTION") != "true":
    REDIRECT_URI = "http://localhost:8501"
else :
    REDIRECT_URI = "https://diplochatai-dha6h6c0fdbhd2ev.centralus-01.azurewebsites.net"
SCOPES = ["User.Read"]
GRAPH_API_URL = "https://graph.microsoft.com/v1.0/me"

msal_app = msal.ConfidentialClientApplication(CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
auth_url = msal_app.get_authorization_request_url(SCOPES, redirect_uri=REDIRECT_URI)



# Session state
if "user" not in st.session_state:
    st.session_state["user"] = None
if "access_token" not in st.session_state:
    st.session_state["access_token"] = None
if "continue_button" not in st.session_state:
    st.session_state["continue_button"] = False

# אם Azure החזיר קוד → מחליפים לטוקן
query_params = st.query_params
if "code" in query_params and st.session_state["access_token"] is None:
    code = query_params["code"]
    token_response = msal_app.acquire_token_by_authorization_code(code, SCOPES, redirect_uri=REDIRECT_URI)
    if "access_token" in token_response:
        st.session_state["access_token"] = token_response["access_token"]
        st.rerun()
    else:
        st.error(f"שגיאה בקבלת טוקן: {token_response}")
        st.stop()

# אם יש טוקן ולא שלפנו עדיין את המשתמש
if st.session_state["access_token"] and not st.session_state["user"]:
    headers = {"Authorization": f"Bearer {st.session_state['access_token']}"}
    user_info = requests.get(GRAPH_API_URL, headers=headers).json()
    
    if "error" in user_info:
        st.error("❌ התחברות נכשלה! נא להתחבר מחדש.")
        st.session_state["access_token"] = None
    else:
        st.session_state["user"] = user_info
        st.rerun()

# אם כבר יש user — נבצע מעבר ל-Diplochat
if st.session_state["user"]:
    encoded_user = urllib.parse.quote(str(st.session_state["user"]))
    st.markdown(f'<meta http-equiv="refresh" content="0; url=/Diplochat?user_data={encoded_user}">', unsafe_allow_html=True)

# UI רגיל עם כפתור התחלה
logo_base64 = get_base64_image("MainLogo.svg")
background_base64 = get_base64_image("Backg.jpg")

with elements("home_page"):
    with mui.Box(  # עטיפה חיצונית למרכז הכל
        sx={
            "display": "flex",
            "flexDirection": "column",
            "alignItems": "center",
            "justifyContent": "center",
            "textAlign": "center"
        }
    ):
        mui.Box(
            component="img",
            src=f"data:image/svg+xml;base64,{logo_base64}",
            sx={
                "width": 550,
                "height": 350,
                "marginBottom": "5px",
                "borderRadius": "20px"  # אופציונלי, נותן עיגול רך
            }
        )

        mui.Button(
            "Continue",
            variant="contained",
            color="secondary",
            onClick=lambda: st.session_state.update({"continue_button": True}),
            sx={
                "marginTop": "5px",
                "padding": "10px 32px",
                "minWidth": "100px",
                "height": "45px",
                "fontSize": "16px",
                "borderRadius": "10px",
                "textTransform": "uppercase",
                "letterSpacing": "1px",
                "background": "linear-gradient(90deg, #7D3C98, #BB86FC)",
                "boxShadow": "0px 5px 15px rgba(125, 60, 152, 0.3)",
                "color": "#fff",
                "fontWeight": "bold",
                "transition": "0.2s ease-in-out"
            }
        )

# כאן ממש כמו אצלך — אם נלחץ על continue נבצע רידיירקט ל-Azure
if st.session_state["continue_button"]:
    st.markdown(f'<meta http-equiv="refresh" content="0; url={auth_url}">', unsafe_allow_html=True)

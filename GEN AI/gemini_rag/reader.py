import os
import re
import base64
import fitz
import tempfile
import streamlit as st
from streamlit_pdf_reader import pdf_reader
from PyPDF2 import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import google.generativeai as genai
from langchain.vectorstores import FAISS
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.chains.question_answering import load_qa_chain
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv
from reportlab.pdfgen import canvas
import cv2
import pytesseract
import tempfile
from PIL import Image
import pytesseract
import streamlit as st
import easyocr
import os
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet


os.environ["KMP_DUPLICATE_LIB_OK"]="TRUE"

# Load API key
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
reader = easyocr.Reader(['en'])  # Specify 'en' for English

# Function to extract text using EasyOCR
def extract_text_from_image(image_path):
    result = reader.readtext(image_path)
    # Extracting the text from the result
    extracted_text = "\n".join([text[1] for text in result])
    return extracted_text

# Function to process PDF and TXT files
def process_files_by_type(files, file_type):
    if file_type == "PDF":
        return get_pdf_text(files)
    elif file_type == "TXT":
        return get_txt_files(files)
    elif file_type == 'IMG':
        return get_png_text(files)

def perform_regex(text):
    without_newlines = re.sub(r'\n', ' ', text)
    return re.sub(r"-\s+", "", without_newlines)

def get_txt_files(txt_files):
    text_chunks_with_metadata = []
    for i, txt_file in enumerate(txt_files):
        page_text = txt_file.read().decode("utf-8")
        if page_text:
            text_chunks_with_metadata.append({"text": page_text, "metadata": {"page": i + 1, "file_name": txt_file.name}})
    return text_chunks_with_metadata

def get_pdf_text(pdf_docs):
    text_chunks_with_metadata = []
    for pdf in pdf_docs:
        file_name = pdf.name if pdf.name else os.path.basename(pdf)
        reader = PdfReader(pdf)
        for i, page in enumerate(reader.pages):
            page_text = page.extract_text()
            if page_text:
                text_chunks_with_metadata.append({
                    "text": page_text,
                    "metadata": {
                        "page": i + 1,
                        "file_name": file_name
                    }
                })
    return text_chunks_with_metadata

def get_png_text(png_files):
    text_chunks_with_metadata = []
    for i, png_file in enumerate(png_files):
                            # Save the PNG image temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as img_temp:
            img_temp.write(png_file.getvalue())
                    
            # Extract text using EasyOCR
            extracted_text = extract_text_from_image(img_temp.name)
            if extracted_text:
                text_chunks_with_metadata.append({
                    "text": extracted_text,
                    "metadata": {
                        "page": i + 1,
                        "file_name": png_file.name
                    }
                })
    return text_chunks_with_metadata




def get_text_chunks(text_with_metadata):
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
    chunks_with_metadata = []

    for item in text_with_metadata:
        
        text = item["text"]
        page_number = item["metadata"]["page"]
        file_name = item["metadata"]["file_name"]
        for chunk in text_splitter.split_text(text):
            chunks_with_metadata.append({"text": chunk, "metadata": {"page": page_number, "file_name": file_name}})
    return chunks_with_metadata

def get_vector_store(chunks_with_metadata):
    embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
    texts = [chunk["text"] for chunk in chunks_with_metadata]
    metadatas = [chunk["metadata"] for chunk in chunks_with_metadata]
    vector_store = FAISS.from_texts(texts, embedding=embeddings, metadatas=metadatas)
    vector_store.save_local("faiss_index")

# Define conversational chain
def get_conversational_chain():
    prompt_template = """
Instruction: Provide a comprehensive and detailed answer to the question using only the information from the context below. If the exact answer is found, explain it thoroughly in your own words. If the direct answer is not available, provide the most relevant information from the context and explain how it relates to the question. If no relevant details are found, respond with: "The answer is not available in the provided context."

Document Starting Text:
{doc_starting_text}

Context:
{context}

Question:
{question}

Answer:
    """
    model = ChatGoogleGenerativeAI(model="gemini-pro", temperature=0.1)
    prompt = PromptTemplate(template=prompt_template, input_variables=["doc_starting_text", "context", "question"])
    return load_qa_chain(model, chain_type="stuff", prompt=prompt)

def user_input(doc_starting_text, user_question, temp_file_path):
    embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
    new_db = FAISS.load_local("faiss_index", embeddings, allow_dangerous_deserialization=True)
    docs = new_db.similarity_search(user_question, k=3)

    chain = get_conversational_chain()
    response = ''
    try:
        response = chain({"input_documents": docs, "doc_starting_text": doc_starting_text, "context": docs, "question": user_question}, return_only_outputs=True)
    except Exception as e:
        print("Error during chain execution:", e)
    
    response_text = response["output_text"]
    page_seen = set()
    relevant_files = set()
    for doc in docs:
        file_name = doc.metadata.get("file_name", "unknown file")
        page_info = doc.metadata.get("page", "unknown page")
        if page_info not in page_seen:
            pass
        page_seen.add(page_info)

        try:
            file_name = file_name.replace('.txt', '.pdf')
            file_name = file_name.replace('.png', '.pdf')
        except:
            print('error')

        relevant_files.add(file_name)

    highlights = [{"page": doc.metadata.get("page"), "text": perform_regex(doc.page_content)} for doc in docs]
    highlight_text_in_pdf(highlights, st.session_state.temp_file_path, list(relevant_files))
    
    return response_text

# Highlight text in the PDF
def highlight_text_in_pdf(highlights, temp_file_paths, relevant_files):
    if not temp_file_paths:
        st.error("Temporary file paths not set.")
        return None

    st.session_state.output_path = []
    print("1"*100)
    print(highlights)
    print('2'*100)
    print(relevant_files)
    print('3'*100)
    print(temp_file_paths)

#     2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
# ['C:\\Users\\MUKUL_~1\\AppData\\Local\\Temp\\tmpwm6z9qla.pdf']
# 3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333
# [{'path': 'C:\\Users\\MUKUL_~1\\AppData\\Local\\Temp\\tmppe03ksbt.pdf', 'file_name': 'sampleimage.pdf'}]


# 2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
# ['gpt4.pdf']
# 3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333
# [{'path': 'C:\\Users\\MUKUL_~1\\AppData\\Local\\Temp\\tmppyt0ffwk.pdf', 'file_name': 'gpt4.pdf'}]


    for file_path in temp_file_paths:
        if file_path['file_name'] in relevant_files:
            try:
                doc = fitz.open(file_path['path'])
                print('4'*100)
                print(doc)
                for highlight in highlights:
                    page_number = highlight["page"]
                    text = highlight["text"][: len(highlight["text"]) // 2]
                    
                    if 0 <= page_number - 1 < doc.page_count:
                        page = doc[page_number - 1]
                        inst = page.search_for(text, quads=True)
                        print("5"*100)
                        print(inst)
                        if inst:
                            page.add_highlight_annot(inst)
                output_path = file_path['path']
                print('6'*100)
                st.session_state.output_path.append(output_path)
                print(st.session_state.output_path)

                doc.save(output_path, incremental=True, encryption=fitz.PDF_ENCRYPT_KEEP)
            except Exception as e:
                st.error(f"An error occurred while processing {file_path}: {str(e)}")
            finally:
                doc.close()

# Convert TXT to PDF using ReportLab
# def convert_txt_to_pdf(txt_content, output_pdf_path):
#     c = canvas.Canvas(output_pdf_path)
#     c.setFont("Helvetica", 12)
#     width, height = 595, 842
#     y_position = height - 50
#     line_spacing = 14
#     for line in txt_content.splitlines():
#         if y_position <= 50:
#             c.showPage()
#             c.setFont("Helvetica", 12)
#             y_position = height - 50
#         c.drawString(50, y_position, line)
#         y_position -= line_spacing
#     c.save()

def convert_txt_to_pdf(txt_content, output_pdf_path):
    # Create a PDF document
    doc = SimpleDocTemplate(output_pdf_path, pagesize=letter)
    
    # Define styles for the text
    styles = getSampleStyleSheet()
    style = styles["Normal"]  # Use the "Normal" style for body text

    # Convert each paragraph in the TXT file into a Paragraph object
    story = []
    for line in txt_content.split("\n"):
        if line.strip():  # Skip empty lines
            story.append(Paragraph(line.strip(), style))
    
    # Build the PDF
    doc.build(story)

    print(f"PDF successfully created: {output_pdf_path}")

def convert_img_to_pdf():
    print("image to pdf converting to store")

# Save uploaded files to local
def save_to_local(docs):
    if "temp_file_path" not in st.session_state:
        st.session_state.temp_file_path = []

    for uploaded_file in docs:
        file_name = uploaded_file.name
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            if file_name.endswith('.pdf'):
                temp_file.write(uploaded_file.getvalue())
                st.session_state.temp_file_path.append({"path": temp_file.name, "file_name": file_name})
            elif file_name.endswith('.txt'):
                try:
                    plain_text = uploaded_file.getvalue().decode('utf-8', errors='replace')
                    convert_txt_to_pdf(plain_text, temp_file.name)
                    st.session_state.temp_file_path.append({"path": temp_file.name, "file_name": file_name.replace('.txt', '.pdf')})
                except Exception as e:
                    st.error(f"Error processing file {file_name}: {e}")
            elif file_name.endswith('.png'):
                try:
                    # Save the PNG image temporarily
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as img_temp:
                        img_temp.write(uploaded_file.getvalue())
                    
                    # Extract text using EasyOCR
                    extracted_text = extract_text_from_image(img_temp.name)

                    # Save the extracted text as a PDF (you can modify this part to suit your requirements)
                    convert_txt_to_pdf(extracted_text, temp_file.name)

                    # Store the generated PDF file path
                    st.session_state.temp_file_path.append({"path": temp_file.name, "file_name": file_name.replace('.png', '.pdf')})

                except Exception as e:
                    st.error(f"Error processing Image file {file_name}: {e}")

            print(st.session_state.temp_file_path)



        

# Streamlit UI setup
def main():
    st.set_page_config(layout="wide")
    left_col, right_col = st.columns([6, 6])
    doc_starting_text = ''
    with left_col:
        st.title("GenixChat ✨ ")
        with st.sidebar:
            st.title("Menu:")
            file_type = st.selectbox("Select file type", options=["PDF", "TXT", "IMG"])
            file_extensions = {"PDF": ["pdf"], "TXT": ["txt"], "IMG":['png']}
            docs = st.file_uploader(f"Upload your {file_type} files", type=file_extensions[file_type], accept_multiple_files=True)
            if st.button("Submit & Process"):
                if docs is not None:
                    with st.spinner(f"Processing {file_type} files..."):
                        save_to_local(docs)
                        raw_text = process_files_by_type(docs, file_type)
                        text_chunks = get_text_chunks(raw_text)
                        doc_starting_text += text_chunks[0]['text']
                        get_vector_store(text_chunks)
                        st.success("Processing complete!")

        if "messages" not in st.session_state:
            st.session_state.messages = []
        with st.expander("Chat History", expanded=True):
            for message in st.session_state.messages:
                with st.container():
                    with st.chat_message(message["role"]):
                        st.markdown(message["content"])

        with st.container():
            if prompt := st.chat_input("What is up?"):
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.markdown(prompt)

                if 'temp_file_path' in st.session_state:
                    response = user_input(doc_starting_text, prompt, st.session_state.temp_file_path)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                    with st.chat_message("assistant"):
                        st.markdown(response)


    # PDF display on the right
    with right_col:
        st.header("PDF Lens")

        if 'output_path' in st.session_state:
            high_file_paths = st.session_state.output_path
            with st.spinner("Loading PDF..."):
                # Read and encode the PDF file
                for high_file_path in high_file_paths:
                    #st.info(f"loading {high_file_path}")
                    with open(high_file_path, "rb") as f:
                        pdf_data = f.read()
                        base64_pdf = base64.b64encode(pdf_data).decode("utf-8")
                        #Display the PDF in an iframe after the spinner
                        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="600px"></iframe>'
                        right_col.markdown(pdf_display, unsafe_allow_html=True)
        else:
            st.info("No file highlighted yet. Please upload a file first.")


if __name__ == "__main__":
    main()

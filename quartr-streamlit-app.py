import streamlit as st
import boto3
import requests
import json
from datetime import datetime
import pandas as pd
import asyncio
import aiohttp
import aioboto3
from typing import List, Dict, Any
import io
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from PIL import Image as PILImage

# Page configuration
st.set_page_config(
    page_title="Quartr Data Retrieval",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []

class WatermarkDocTemplate(SimpleDocTemplate):
    """Custom document template with watermark support"""
    def __init__(self, filename, logo_data=None, logo_opacity=0.1, **kwargs):
        super().__init__(filename, **kwargs)
        self.logo_data = logo_data
        self.logo_opacity = logo_opacity

    def handle_nextPage(self):
        if self.logo_data:
            canvas = self.canv
            canvas.saveState()
            
            page_width, page_height = letter
            canvas.setFillAlpha(self.logo_opacity)
            
            x = page_width/2
            y = page_height/2
            
            try:
                img = Image(self.logo_data)
                max_width = 3 * inch
                aspect = img.imageHeight / float(img.imageWidth)
                
                if img.imageWidth > max_width:
                    img._width = max_width
                    img._height = max_width * aspect
                else:
                    img._width = img.imageWidth
                    img._height = img.imageHeight
                
                img.drawOn(canvas, x - img._width/2, y - img._height/2)
                
            except Exception as e:
                st.error(f"Error adding watermark: {str(e)}")
                
            canvas.restoreState()
        
        super().handle_nextPage()

class TranscriptProcessor:
    @staticmethod
    async def process_transcript(transcript_url: str, session: aiohttp.ClientSession) -> str:
        """Process transcript JSON into clean text"""
        async with session.get(transcript_url) as response:
            if response.status == 200:
                try:
                    transcript_data = await response.json()
                    return transcript_data.get('transcript', {}).get('text', '')
                except json.JSONDecodeError:
                    st.error(f"Error decoding transcript JSON from {transcript_url}")
                    return ''
            return ''

    @staticmethod
    def create_pdf(company_name: str, event_title: str, event_date: str, 
                   transcript_text: str, logo_url: str = None, logo_opacity: float = 0.1) -> bytes:
        """Create a PDF with watermark from transcript text"""
        buffer = io.BytesIO()
        
        # Get logo data if URL provided
        logo_data = None
        if logo_url:
            try:
                response = requests.get(logo_url)
                response.raise_for_status()
                logo_data = response.content
            except Exception as e:
                st.error(f"Error fetching logo: {str(e)}")

        # Create PDF with watermark
        doc = WatermarkDocTemplate(
            buffer,
            logo_data=logo_data,
            logo_opacity=logo_opacity,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )

        styles = getSampleStyleSheet()
        
        header_style = ParagraphStyle(
            'CustomHeader',
            parent=styles['Heading1'],
            fontSize=14,
            spaceAfter=30,
            textColor=colors.HexColor('#1a472a'),
            alignment=1
        )
        
        speaker_style = ParagraphStyle(
            'Speaker',
            parent=styles['Heading2'],
            fontSize=11,
            textColor=colors.HexColor('#666666'),
            spaceBefore=20,
            spaceAfter=10,
            fontName='Helvetica-Bold'
        )
        
        text_style = ParagraphStyle(
            'CustomText',
            parent=styles['Normal'],
            fontSize=10,
            leading=14,
            spaceBefore=6,
            fontName='Helvetica'
        )

        story = []
        
        # Add header
        header_text = f"""
            <para alignment="center">
            <b>{company_name}</b><br/>
            <br/>
            Event: {event_title}<br/>
            Date: {event_date}
            </para>
        """
        story.append(Paragraph(header_text, header_style))
        story.append(Spacer(1, 30))

        # Process transcript text
        paragraphs = transcript_text.strip().split('\n\n')
        for para in paragraphs:
            if para.strip():
                if para.strip().startswith('['):
                    story.append(Paragraph(para, speaker_style))
                else:
                    story.append(Paragraph(para, text_style))
                story.append(Spacer(1, 6))

        doc.build(story)
        return buffer.getvalue()

class QuartrAPI:
    def __init__(self):
        self.api_key = st.secrets["quartr"]["API_KEY"]
        self.base_url = "https://api.quartr.com/public/v1"
        self.headers = {"X-Api-Key": self.api_key}
        
    async def get_company_events(self, isin: str, session: aiohttp.ClientSession) -> Dict:
        url = f"{self.base_url}/companies/isin/{isin}"
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    st.error(f"Error fetching data for ISIN {isin}: {response.status}")
                    return {}
        except Exception as e:
            st.error(f"Error fetching data for ISIN {isin}: {str(e)}")
            return {}

class S3Handler:
    def __init__(self):
        self.session = aioboto3.Session(
            aws_access_key_id=st.secrets["aws"]["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets["aws"]["AWS_DEFAULT_REGION"]
        )

    async def upload_file(self, file_data: bytes, s3_key: str, bucket_name: str, 
                         content_type: str = 'application/pdf'):
        try:
            async with self.session.client('s3') as s3:
                await s3.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=file_data,
                    ContentType=content_type
                )
                return True
        except Exception as e:
            st.error(f"Error uploading to S3: {str(e)}")
            return False

def format_s3_key(company_name: str, date: str, doc_type: str, filename: str) -> str:
    """Format S3 key with proper naming convention"""
    clean_company = company_name.replace(" ", "_").replace("/", "_").lower()
    clean_date = date.split("T")[0]
    clean_filename = filename.replace(" ", "_").lower()
    return f"{clean_company}/{clean_date}/{doc_type}/{clean_filename}"

async def process_documents(isin_list: List[str], start_date: str, end_date: str, 
                          selected_docs: List[str], bucket_name: str):
    quartr = QuartrAPI()
    s3_handler = S3Handler()
    transcript_processor = TranscriptProcessor()
    
    # Get logo configuration from secrets
    logo_url = st.secrets["branding"]["COMPANY_LOGO_URL"]
    logo_opacity = float(st.secrets["branding"]["LOGO_OPACITY"])
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    files_processed = st.empty()
    
    total_files = 0
    processed_files = 0
    successful_uploads = 0
    failed_uploads = 0
    
    try:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for isin in isin_list:
                tasks.append(quartr.get_company_events(isin, session))
            
            companies_data = await asyncio.gather(*tasks)
            
            # Calculate total files
            for company in companies_data:
                if not company:
                    continue
                for event in company.get('events', []):
                    event_date = event.get('eventDate', '').split('T')[0]
                    if start_date <= event_date <= end_date:
                        for doc_type in selected_docs:
                            if event.get(f'{doc_type}Url'):
                                total_files += 1
            
            if total_files == 0:
                st.warning("No matching documents found for the specified criteria.")
                return
            
            # Process files
            for company in companies_data:
                if not company:
                    continue
                    
                company_name = company.get('displayName', 'unknown')
                
                for event in company.get('events', []):
                    event_date = event.get('eventDate', '').split('T')[0]
                    event_title = event.get('eventTitle', 'Unknown Event')
                    
                    if start_date <= event_date <= end_date:
                        for doc_type in selected_docs:
                            file_url = event.get(f'{doc_type}Url')
                            if file_url:
                                success = False
                                
                                if doc_type == 'transcript':
                                    transcript_text = await transcript_processor.process_transcript(
                                        file_url, 
                                        session
                                    )
                                    if transcript_text:
                                        pdf_bytes = transcript_processor.create_pdf(
                                            company_name,
                                            event_title,
                                            event_date,
                                            transcript_text,
                                            logo_url=logo_url,
                                            logo_opacity=logo_opacity
                                        )
                                        
                                        s3_key = format_s3_key(
                                            company_name,
                                            event_date,
                                            doc_type,
                                            f"{event_title.lower().replace(' ', '_')}_transcript.pdf"
                                        )
                                        
                                        success = await s3_handler.upload_file(
                                            pdf_bytes,
                                            s3_key,
                                            bucket_name
                                        )
                                else:
                                    # Handle regular files (slides, reports)
                                    async with session.get(file_url) as response:
                                        if response.status == 200:
                                            content = await response.read()
                                            s3_key = format_s3_key(
                                                company_name,
                                                event_date,
                                                doc_type,
                                                file_url.split('/')[-1]
                                            )
                                            success = await s3_handler.upload_file(
                                                content,
                                                s3_key,
                                                bucket_name,
                                                response.headers.get('content-type', 'application/pdf')
                                            )
                                
                                if success:
                                    successful_uploads += 1
                                else:
                                    failed_uploads += 1
                                
                                processed_files += 1
                                progress = processed_files / total_files
                                progress_bar.progress(progress)
                                status_text.text(f"Processing: {processed_files}/{total_files} files")
                                files_processed.text(
                                    f"Successful uploads: {successful_uploads} | "
                                    f"Failed uploads: {failed_uploads}"
                                )
                                
                                await asyncio.sleep(0.1)
            
            progress_bar.progress(1.0)
            status_text.text("Processing complete!")
            files_processed.text(
                f"Final results:\n"
                f"Total files processed: {processed_files}\n"
                f"Successful uploads: {successful_uploads}\n"
                f"Failed uploads: {failed_uploads}"
            )
            
            st.session_state.processing_complete = True
            
    except Exception as e:
        st.error(f"An error occurred during processing: {str(e)}")
        raise

def main():
    st.title("Quartr Data Retrieval and S3 Upload")
    
    # Example ISINs
    st.sidebar.header("Help")
    st.sidebar.markdown("""
    ### Example ISINs:
    - US5024413065 (LVMH ADR)
    - FR0000121014 (LVMH)
    - TH0809120700 (LVMH TH)
    
    Enter one ISIN per line in the input box.
    """)
    
    with st.form(key="quartr_form"):
        isin_input = st.text_area(
            "Enter ISINs (one per line)",
            help="Enter each ISIN on a new line. See sidebar for examples.",
            height=100
        )
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                datetime(2024, 1, 1),
                help="Select start date for document retrieval",
                min_value=datetime(2000, 1, 1)
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                datetime(2024, 12, 31),
                help="Select end date for document retrieval",
                max_value=datetime(2025, 12, 31)
            )
        
        doc_types = st.multiselect(
            "Select document types",
            ["slides", "report", "transcript"],
            default=["slides", "report", "transcript"],
            help="Choose which types of documents to retrieve"
        )
        
        # Get default bucket from secrets with fallback
        default_bucket = ""
        try:
            default_bucket = st.secrets["s3"]["DEFAULT_BUCKET"]
        except Exception:
            st.warning("No default bucket configured in secrets.toml")
            
        s3_bucket = st.text_input(
            "S3 Bucket Name",
            value=default_bucket,
            help="Enter the name of the S3 bucket for file upload"
        )
        
        # Submit button must be the last element in the form
        submitted = st.form_submit_button("Start Processing")
        
        if submitted:
            if not isin_input or not s3_bucket or not doc_types:
                st.error("Please fill in all required fields")
                return
            
            if start_date > end_date:
                st.error("Start date must be before end date")
                return
            
            isin_list = [isin.strip() for isin in isin_input.split("\n") if isin.strip()]
            
            if not isin_list:
                st.error("Please enter at least one valid ISIN")
                return
            
            try:
                asyncio.run(process_documents(
                    isin_list,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    doc_types,
                    s3_bucket
                ))
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                return

if __name__ == "__main__":
    main()
Last edited just now

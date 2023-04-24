import streamlit as st
from ksql import KSQLAPI
import openai
import pinecone
import altair as alt
import pandas as pd

@st.cache_resource
def initialize():
    openai.api_key = st.secrets['openai_key']
    index_name = 'openai-embedding-data'

    # initialize connection to pinecone (get API key at app.pinecone.io)
    pinecone.init(
        api_key=st.secrets['pinecone_key'],
        environment=st.secrets['pinecone_env']  # may be different, check at app.pinecone.io
    )
    # connect to index
    index = pinecone.Index(index_name)

    client = KSQLAPI(st.secrets('ksql_address'), 
                         api_key=st.secrets('ksql_API_key'), 
                         secret=st.secrets('ksql_API_secret'))
    return index, client
index, client = initialize()

def complete(prompt):
    # query text-davinci-003
    res = openai.Completion.create(
        engine='text-davinci-003',
        prompt=prompt,
        temperature=0,
        max_tokens=400,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )
    return res['choices'][0]['text'].strip()

limit = 3750
embed_model = "text-embedding-ada-002"
def retrieve(query):
    res = openai.Embedding.create(
        input=[query],
        engine=embed_model
    )

    # retrieve from Pinecone
    xq = res['data'][0]['embedding']

    # get relevant contexts
    res = index.query(xq, top_k=3, include_metadata=True)
    contexts = [
        x['metadata']['text'] for x in res['matches']
    ]
    titles = [
        x['metadata']['title'] for x in res['matches']
    ]
    urls = [
            x['metadata']['url'] for x in res['matches']
        ]

    # build our prompt with the retrieved contexts included
    prompt_start = (
        "Answer the question based on the context below.\n\n"+
        "Context:\n"
    )
    prompt_end = (
        f"\n\nQuestion: {query}\nAnswer:"
    )
    # append contexts until hitting limit
    for i in range(1, len(contexts)):
        if len("\n\n---\n\n".join(contexts[:i])) >= limit:
            prompt = (
                prompt_start +
                "\n\n---\n\n".join(contexts[:i-1]) +
                prompt_end
            )
            break
        elif i == len(contexts)-1:
            prompt = (
                prompt_start +
                "\n\n---\n\n".join(contexts) +
                prompt_end
            )
    return prompt, titles, urls

# UI goes here
@st.cache_data
def load_data():
    ksql_responce = client.query('''select hashtag, count(*) as counts 
                                        from tweets_table
                                        group by hashtag''')
    data = [x['row']['columns']  for x in ksql_responce]
    df = pd.DataFrame(data, columns=['HashTag', 'Exposure'])
    return df
df = load_data()


# Create the Altair chart
chart = alt.Chart(df).mark_bar().encode(
    x='HashTag',
    y='Exposure'
)

# Display the chart in Streamlit
st.altair_chart(chart, use_container_width=True)

_,col1,_ = st.columns([1,8,1])
with col1: 
    form = st.form(key='myform')
    query = form.text_input( "Enter some text 👇",
        placeholder="Write your prompt here...",
    )
    submit = form.form_submit_button('Submit')
if submit:
    # get context, additional info from pinecone
    query_with_contexts, titles, urls = retrieve(query)
    # call openai API
    output = complete(query_with_contexts)
    with st.expander("See contexts prompt from the RAG"):
        st.write(query_with_contexts)
    st.write('#### '+output)
    st.write('### '+ 'Reference')
    for i, (title, url) in enumerate(zip(titles, urls)):
        st.write(f'[{str(i+1)}. {title}]({url})')

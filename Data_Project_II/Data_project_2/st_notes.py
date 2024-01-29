import streamlit as st

# terminal -> streamlit run streamlit.py –> se abre una pestaña localhost en el navegador
st.title("This is the Data Project2 Web App")
st.markdown("---")
# st.subheader(); st.header(); st.text(); st.markdown();
st.text("Objectives for now:")
st.markdown("> Show the vlc map from the html doc")

#st.checkbox("", value=True/False)
st.radio("In which country do you live?", options = ("UK", "US", "Canada"))
select = st.selectbox("Which is your favourite car brand?", options = ("Audi", "BMW", "Ferrari"))
st.write(select)
#st.multiselect()



# video remove hamburguer&footer

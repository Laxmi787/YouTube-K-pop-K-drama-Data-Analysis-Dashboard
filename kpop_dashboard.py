import streamlit as st
import pandas as pd
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import re
from textblob import TextBlob

st.set_page_config(page_title="K-Pop Dashboard", layout="wide")

# Load Data
data = pd.read_csv("kpop_drama.csv")
data['published_at'] = pd.to_datetime(data['published_at'])

# Combine pinned and top comments for sentiment
data['all_comments'] = data['pinned_comment'].fillna('') + ' ' + data['top_comments'].fillna('')

# Filter out rows with no comments for sentiment
sentiment_df = data[data['all_comments'].str.strip() != ''].copy()

# Compute sentiment polarity
def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

sentiment_df['sentiment_score'] = sentiment_df['all_comments'].apply(get_sentiment)

# Classify sentiment
def label_sentiment(score):
    if score > 0.1:
        return 'Positive'
    elif score < -0.1:
        return 'Negative'
    else:
        return 'Neutral'

sentiment_df['sentiment_label'] = sentiment_df['sentiment_score'].apply(label_sentiment)

# Merge sentiment_label back to main data where possible
data = data.merge(sentiment_df[['all_comments', 'sentiment_label']], on='all_comments', how='left')

# Use 'sentiment_label' from merged data or fill NA with 'Neutral'
data['sentiment_label'] = data['sentiment_label'].fillna('Neutral')

df = data.copy()

# Preprocess
df['Month'] = df['published_at'].dt.to_period('M').astype(str)
df['Hour'] = df['published_at'].dt.hour
df['day_of_week'] = df['published_at'].dt.day_name()

# Sidebar Filters
st.sidebar.header("🔎 Filters")
channels = st.sidebar.multiselect("Select Channels", df['channel_name'].unique(), default=df['channel_name'].unique())
min_views = st.sidebar.slider("Minimum Views", 0, int(df['views'].max()), 10000)
date_range = st.sidebar.date_input("Published Between", [df['published_at'].min().date(), df['published_at'].max().date()])

filtered_df = df[
    (df['channel_name'].isin(channels)) &
    (df['views'] >= min_views) &
    (df['published_at'].dt.date.between(date_range[0], date_range[1]))
].copy()

# --- KPI Section ---
st.title("K-Pop & Drama Realtime Dashboard")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Videos", f"{len(filtered_df):,}")
col2.metric("Total Views", f"{filtered_df['views'].sum():,}")
col3.metric("Total Likes", f"{filtered_df['likes'].sum():,}")
engagement_rate = ((filtered_df['likes'] + filtered_df['comments']).sum() / filtered_df['views'].sum()) * 100 if filtered_df['views'].sum() > 0 else 0
col4.metric("Engagement Rate", f"{engagement_rate:.2f}%")

# --- Tabs Layout ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Trends", "🏷️ Hashtags", "🔮 Insights"])

with tab1:
    st.subheader("Top 10 Videos")
    st.dataframe(filtered_df.sort_values(by="views", ascending=False)[['title', 'channel_name', 'views', 'likes', 'comments', 'url']].head(10))

    st.subheader("Views by Channel")
    channel_views = filtered_df.groupby('channel_name')['views'].sum().reset_index().sort_values(by='views', ascending=False)
    fig1 = px.bar(channel_views, x='channel_name', y='views', title='Total Views by Channel', color='views')
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Engagement by Hour")
    # Format hours as AM/PM for display
    filtered_df['Hour_AMPM'] = filtered_df['Hour'].apply(lambda h: f"{(h%12) if (h%12)!=0 else 12}{' AM' if h < 12 else ' PM'}")
    fig2 = px.box(filtered_df, x='Hour_AMPM', y='likes', category_orders={"Hour_AMPM": ['12 AM','1 AM','2 AM','3 AM','4 AM','5 AM','6 AM','7 AM','8 AM','9 AM','10 AM','11 AM','12 PM','1 PM','2 PM','3 PM','4 PM','5 PM','6 PM','7 PM','8 PM','9 PM','10 PM','11 PM']}, title='Likes by Hour of Posting (AM/PM)')
    fig2.update_xaxes(title='Hour of Day')
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.subheader("📆 Monthly View Trends")
    monthly = filtered_df.groupby('Month')['views'].sum().reset_index()
    fig3 = px.line(monthly, x='Month', y='views', markers=True, title="Monthly View Trend")
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("📌 Views vs Likes Scatter")
    fig4 = px.scatter(filtered_df, x='views', y='likes', color='channel_name', hover_data=['title'], title="Views vs Likes")
    st.plotly_chart(fig4, use_container_width=True)

    st.subheader("🎞️ Duration vs Views")
    bins = [0, 180, 360, 600, 1200, 1800, 9999]
    labels = ['0–3 min', '3–6 min', '6–10 min', '10–20 min', '20–30 min', '30+ min']
    filtered_df['duration_bin'] = pd.cut(filtered_df['duration_seconds'], bins=bins, labels=labels, include_lowest=True)

    duration_avg = filtered_df.groupby('duration_bin')['views'].mean().reset_index()
    fig5 = px.bar(duration_avg, x='duration_bin', y='views',
                  title='📺 Average Views by Video Duration',
                  color='views', color_continuous_scale='blues')
    st.plotly_chart(fig5, use_container_width=True)

    st.subheader("📊 Sentiment Distribution")
    sentiment_counts = filtered_df['sentiment_label'].value_counts().reset_index()
    sentiment_counts.columns = ['Sentiment', 'Count']
    fig_sent = px.pie(sentiment_counts, names='Sentiment', values='Count', title="Viewer Sentiment Distribution")
    st.plotly_chart(fig_sent, use_container_width=True)

    st.subheader("📆 Engagement by Day of Week")
    day_engagement = filtered_df.groupby('day_of_week')['views'].mean().reset_index()
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_engagement['day_of_week'] = pd.Categorical(day_engagement['day_of_week'], categories=days_order, ordered=True)
    day_engagement = day_engagement.sort_values('day_of_week')

    fig_day = px.bar(day_engagement, x='day_of_week', y='views', color='views',
                     title='Average Views by Day of the Week')
    st.plotly_chart(fig_day, use_container_width=True)

    st.subheader("🕒 Engagement by Hour of Day")
    hour_engagement = filtered_df.groupby('Hour')['views'].mean().reset_index()
    hour_engagement['Hour_AMPM'] = hour_engagement['Hour'].apply(lambda h: f"{(h%12) if (h%12)!=0 else 12}{' AM' if h < 12 else ' PM'}")
    hour_engagement = hour_engagement.sort_values('Hour')
    fig_hour = px.line(hour_engagement, x='Hour_AMPM', y='views', markers=True, title='Average Views by Hour of Publishing')
    st.plotly_chart(fig_hour, use_container_width=True)

with tab3:
    st.subheader("📢 Hashtag Word Cloud")
    hashtags = " ".join(filtered_df['hashtags'].dropna().astype(str).str.replace('#', '').str.replace('no hastag', ''))
    wordcloud = WordCloud(width=800, height=400, background_color='black').generate(hashtags)
    fig, ax = plt.subplots()
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    st.pyplot(fig)

    st.subheader("🔥 Trending Hashtags Over Time (Top 5)")
    filtered_df['hashtags_clean'] = filtered_df['hashtags'].apply(lambda x: re.findall(r"#\w+", str(x)))
    tag_df = filtered_df.explode('hashtags_clean')
    tag_df = tag_df[tag_df['hashtags_clean'].str.lower() != '#no']
    top_tags = tag_df['hashtags_clean'].value_counts().head(5).index.tolist()

    top_tag_counts = tag_df[tag_df['hashtags_clean'].isin(top_tags)].groupby(['hashtags_clean', 'Month']).size().reset_index(name='count')
    fig_tags = px.line(top_tag_counts, x='Month', y='count', color='hashtags_clean',
                       title="Top 5 Hashtags Trending Over Time")
    st.plotly_chart(fig_tags, use_container_width=True)

with tab4:
    st.subheader("🔮 Content Insights & Strategic Recommendations")

    if not filtered_df.empty:
        best_hour = filtered_df['Hour'].value_counts().idxmax()
    else:
        best_hour = 9  # fallback default

    most_common_tags = ", ".join(top_tags) if top_tags else "N/A"

    sentiment_perc = filtered_df['sentiment_label'].value_counts(normalize=True) * 100
    positive_pct = sentiment_perc.get('Positive', 0)
    neutral_pct = sentiment_perc.get('Neutral', 0)
    negative_pct = sentiment_perc.get('Negative', 0)

    st.markdown(f"""
    ### 📌 Observations
    - Short videos (3–10 minutes) tend to attract the most views.
    - Emotional or fan-centric hashtags like **#btsarmy**, **#blink** lead to higher comments.
    - Morning hours, especially **9–10 AM**, show peak viewership.
    - **BANGTANTV** dominates in views, followed by **BLACKPINK** and **JYP Entertainment**.
    - Channels like **JTBC** and **The Swoon** follow event-driven drama trends.
    - **#jtbc뉴스룸** saw a major spike recently, possibly due to a viral broadcast.
    - **"no hashtag"** content has grown over time, indicating more natural uploads.
    - Positive sentiment ({positive_pct:.1f}%) and neutral sentiment ({neutral_pct:.1f}%) dominate. Negative sentiment is almost negligible ({negative_pct:.1f}%).
    - Words like "love", "song", "cute", "visual", and "vocal" are top terms in comments.

    ### 💡 Recommendations
    - 🕒 **Best Time to Post:** Between **9–10 AM** for maximum viewership.
    - 📹 **Video Length:** Keep videos between **3–10 minutes** to attract higher views.
    - 🔥 **Hashtag Strategy:** Use emotional or fan-driven hashtags like **#btsarmy**, **#blink** for better engagement.
    - 🎯 **Channel Focus:** Collaborate or target audiences similar to **BANGTANTV**, **BLACKPINK**, and **JYP Entertainment** for wider reach.
    - 📅 **Event-Driven Content:** Align content with drama or event trends like those on **JTBC** and **The Swoon**.
    - 🤖 **Natural Uploads:** Don't shy away from “no hashtag” posts; their growth shows audience preference for organic content.
    - 😊 **Sentiment Focus:** Encourage positive and neutral engagement; very low negativity is a good sign.
    """)


